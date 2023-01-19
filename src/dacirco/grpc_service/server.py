"""The GRPC DaCirco server.

It receives the requests from the REST front-end and it forwards them to the scheduler.

.. warning::
    Students are not supposed to modify this code

"""


import asyncio
import logging
import subprocess
import sys
import uuid
from asyncio.tasks import sleep
from enum import Enum, auto

import grpc
from dacirco.proto import dacirco_pb2_grpc
from dacirco.proto.dacirco_pb2 import (
    Empty,
    GrpcErrorEvent,
    GrpcEvent,
    RequestIDList,
    RequestID,
    TCRequest,
    TCRequestReply,
    TCRequestStatus,
    TCTask,
    WorkerDesc,
    gRPCServiceReply,
    WorkerID,
    WorkerIDList,
    WorkerFullDesc,
    WorkerGrpcState,
)
from dacirco.scheduler.dacirco_dataclasses import (
    MinIoParameters,
    PodParameters,
    RunnerType,
    SchedulerParameters,
    TCTaskDesc,
    VMParameters,
)
from dacirco.grpc_service.events import TCWorkerErrorEvent, TCWorkerEvent
from dacirco.scheduler.request_tracker import RequestStatus
from dacirco.scheduler.worker_tracker import WorkerStatus
from dacirco.scheduler.scheduler import Scheduler

_logger = logging.getLogger(__name__)
# Coroutines to be invoked when the event loop is shutting down.
_cleanup_coroutines = []


def get_local_ip_address() -> str:
    out1 = subprocess.run(
        ["ip", "-o", "link", "show"], stdout=subprocess.PIPE
    ).stdout.decode("utf-8")
    interfaces = [x.split(":")[1].strip() for x in out1.split("\n")[:-1]]
    for interface in interfaces:
        try:
            out2 = subprocess.run(
                ["ip", "-o", "-4", "addr", "show", interface], stdout=subprocess.PIPE
            ).stdout.decode("utf-8")
            ip_a = out2.split()[3].split("/")[0]
            _logger.debug("Found ip address: %s", ip_a)
            if "10.35" in ip_a:
                _logger.info("Found IP address in 10.35: %s", ip_a)
                return ip_a
        except:
            _logger.critical("Cannot auto-detect the local IP address to use")
            raise SystemExit
    _logger.critical(
        "Trying to run the controller on a machine that does not have an IP address in 10.35, aborting"
    )
    sys.exit()
    return ""


class DaCircoRCPServer(dacirco_pb2_grpc.DaCircogRPCServiceServicer):
    """DaCirco RPC Server"""

    def __init__(
        self,
        vm_parameters: VMParameters,
        minio_parameters: MinIoParameters,
        local_ip_address: str,
        event_log_file: str,
        sched_parameters: SchedulerParameters,
        pod_parameters: PodParameters,
    ) -> None:
        super().__init__()
        self._scheduler = Scheduler(
            vm_parameters=vm_parameters,
            pod_parameters=pod_parameters,
            minio_parameters=minio_parameters,
            sched_parameters=sched_parameters,
            local_ip_address=local_ip_address,
            event_log_file=event_log_file,
        )
        self._call_interval = sched_parameters.call_interval
        self._async_queues: dict[str, asyncio.Queue[TCTask]] = {}

    async def submit_request(
        self, request: TCRequest, context: grpc.aio.ServicerContext  # type: ignore
    ) -> TCRequestReply:
        """Submit Request endpoint

        The REST front end calls this method every time it receives a new request.

        Args:
            request (dacirco_pb2.TCRequest): The TCRequest object (see :doc:`../proto`)
            context (grpc.aio.ServicerContext): The gRPC context

        Returns:
            dacirco_pb2.TCRequestReply: The reply object (see)
        """
        _logger.info(
            "Received TC request from the REST frontend. input_video: %s bitrate: %i speed %s",
            request.input_video,
            request.bitrate,
            request.speed,
        )
        request_id = str(uuid.uuid4())
        sched_res = self._scheduler.new_request(
            request.input_video,
            request.output_video,
            request.bitrate,
            request.speed,
            request_id,
        )
        if sched_res.success:
            res = TCRequestReply(success=True, error_message="", request_id=request_id)
            if sched_res.tc_task_desc:
                _logger.debug(
                    "Sending task to worker: %s , video: %s",
                    sched_res.tc_task_desc.worker_id,
                    sched_res.tc_task_desc.request_desc.input_video,
                )
                self.send_tc_task_to_worker(
                    worker_id=sched_res.tc_task_desc.worker_id,
                    tc_task_desc=sched_res.tc_task_desc,
                )
        else:
            error_message = "No error message given"
            if sched_res.error_message:
                error_message = sched_res.error_message
            else:
                _logger.error("Scheduler returned failure but no error mesage given")
            res = TCRequestReply(
                success=False, error_message=error_message, request_id=request_id
            )
            _logger.error(
                "Scheduler new_request failed, error message %s", res.error_message
            )
        return res

    async def register_worker(
        self, worker_desc: WorkerDesc, context: grpc.aio.ServicerContext  # type: ignore
    ) -> gRPCServiceReply:
        _logger.info(
            "TC Worker registration.  Name: %s, Id: %s",
            worker_desc.name,
            worker_desc.id,
        )
        res = gRPCServiceReply(success=True)
        if worker_desc.id in self._async_queues.keys():
            msg = f"====EE===== worker registering again. Id: {worker_desc.id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            self._async_queues[worker_desc.id] = asyncio.Queue[TCTask]()
            sched_res = self._scheduler.register_worker(
                worker_desc.name, worker_desc.id
            )
            if sched_res.tc_task_desc:
                self.send_tc_task_to_worker(
                    worker_id=worker_desc.id, tc_task_desc=sched_res.tc_task_desc
                )
        return res

    async def get_tasks(self, request: WorkerDesc, context: grpc.aio.ServicerContext):  # type: ignore
        worker_id = request.id
        queue = self._async_queues[worker_id]
        while True:
            tc_task = await queue.get()
            _logger.debug(
                "get_tasks: sending tc task to: %s, input_video: %s",
                worker_id,
                tc_task.input_video,
            )
            yield tc_task

    def send_tc_task_to_worker(self, worker_id: str, tc_task_desc: TCTaskDesc) -> None:
        _logger.debug(
            "Enqueueing message for tc task to: %s, input_video: %s",
            worker_id,
            tc_task_desc.request_desc.input_video,
        )
        queue = self._async_queues[worker_id]
        queue.put_nowait(
            TCTask(
                input_video=tc_task_desc.request_desc.input_video,
                output_video=tc_task_desc.request_desc.output_video,
                bitrate=tc_task_desc.request_desc.bitrate,
                speed=tc_task_desc.request_desc.speed,
                worker_id=tc_task_desc.worker_id,
                task_id=tc_task_desc.task_id,
            )
        )

    async def submit_event(
        self, request: GrpcEvent, context: grpc.aio.ServicerContext  # type: ignore
    ) -> gRPCServiceReply:
        if request.event_type is not GrpcEvent.KEEPALIVE:
            _logger.debug(
                "New Event. Type: %s, worker: %s, task: %s",
                request.event_type,
                request.worker_id,
                request.task_id,
            )
        res = gRPCServiceReply(success=True)
        if not request.worker_id in self._async_queues.keys():
            msg = f"====EE===== Event from unknown worker. Id: {request.worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            res_process_event = self._scheduler.process_event(TCWorkerEvent(request))

            res = gRPCServiceReply(success=True)
            if res_process_event.tc_task_desc:
                _logger.debug(
                    "------------Sending task to worker: %s , video: %s",
                    request.worker_id,
                    res_process_event.tc_task_desc.request_desc.input_video,
                )
                self.send_tc_task_to_worker(
                    worker_id=request.worker_id,
                    tc_task_desc=res_process_event.tc_task_desc,
                )
        return res

    async def submit_error(
        self, request: GrpcErrorEvent, context: grpc.aio.ServicerContext  # type: ignore
    ) -> gRPCServiceReply:
        _logger.debug(
            "===\u274C Error. Type: %s, worker: %s, task: %s, error_message: %s",
            request.error_type,
            request.worker_id,
            request.task_id,
            request.error_message,
        )
        res = gRPCServiceReply(success=True)
        if not request.worker_id in self._async_queues.keys():
            msg = f"====EE===== Event from unknown worker. Id: {request.worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            self._scheduler.process_error_event(TCWorkerErrorEvent(request))
        return res

    async def get_requests(
        self, request: Empty, context: grpc.aio.ServicerContext  # type: ignore
    ) -> RequestIDList:
        return RequestIDList(request_ids=self._scheduler.get_requests())  # type: ignore

    async def get_request(
        self, request: RequestID, context: grpc.aio.ServicerContext  # type: ignore
    ) -> TCRequest:
        req_desc = self._scheduler.get_request(request.request_id)
        if req_desc:
            res = TCRequest(
                input_video=req_desc.input_video,
                bitrate=req_desc.bitrate,
                speed=req_desc.speed,
            )
        else:
            res = TCRequest(
                input_video="",
                bitrate=0,
                speed="",
            )
        return res

    async def get_request_status(
        self, request: RequestID, context: grpc.aio.ServicerContext  # type: ignore
    ) -> TCRequestStatus:
        res = TCRequestStatus.INVALID
        status = self._scheduler.get_request_status(request.request_id)
        if status is RequestStatus.COMPLETED:
            res = TCRequestStatus.COMPLETED
        if status is RequestStatus.WAITING:
            res = TCRequestStatus.WAITING
        if status is RequestStatus.STARTED:
            res = TCRequestStatus.STARTED
        if status is RequestStatus.ERROR:
            res = TCRequestStatus.ERROR
        if status is RequestStatus.NOT_FOUND:
            res = TCRequestStatus.NOT_FOUND
        return TCRequestStatus(request_status=res)

    async def get_workers(
        self, request: Empty, context: grpc.aio.ServicerContext  # type: ignore
    ) -> WorkerIDList:
        return WorkerIDList(worker_ids=self._scheduler.get_workers())  # type: ignore

    async def get_worker(
        self, worker_id: WorkerID, context: grpc.aio.ServicerContext  # type: ignore
    ) -> WorkerFullDesc:
        worker_desc = self._scheduler.get_worker(worker_id=worker_id.worker_id)
        if worker_desc:
            res = WorkerFullDesc(
                name=worker_desc.name,
                id=worker_desc.id,
                cpus=str(worker_desc.cpus),
                memory=str(worker_desc.memory),
                node=worker_desc.node,
            )
        else:
            res = WorkerFullDesc(name="", id="", cpus="", memory="", node="")
        return res

    async def get_worker_status(
        self, worker_id: WorkerID, context: grpc.aio.ServicerContext  # type: ignore
    ) -> WorkerGrpcState:
        res = WorkerGrpcState.INVALID
        status = self._scheduler.get_worker_status(worker_id=worker_id.worker_id)
        if status is WorkerStatus.BOOTING:
            res = WorkerGrpcState.BOOTING
        if status is WorkerStatus.DOWNLOADING_FILE:
            res = WorkerGrpcState.DOWNLOADING_FILE
        if status is WorkerStatus.TRANSCODING:
            res = WorkerGrpcState.TRANSCODING
        if status is WorkerStatus.UPLOADING_FILE:
            res = WorkerGrpcState.UPLOADING_FILE
        if status is WorkerStatus.IDLE:
            res = WorkerGrpcState.IDLE
        if status is WorkerStatus.STOPPED:
            res = WorkerGrpcState.STOPPED
        if status is WorkerStatus.NOT_FOUND:
            res = WorkerGrpcState.NOT_FOUND
        return WorkerGrpcState(worker_status=res)

    async def periodic_call(self) -> None:
        await asyncio.sleep(2)
        while True:
            self._scheduler.periodic_function()
            await asyncio.sleep(self._call_interval)


async def serve(
    vm_parameters: VMParameters,
    minio_parameters: MinIoParameters,
    port: int,
    ip_address: str,
    event_log_file: str,
    sched_parameters: SchedulerParameters,
    pod_parameters: PodParameters,
) -> None:
    server = grpc.aio.server()  # type: ignore
    if ip_address:
        local_ip_address = ip_address
    else:
        local_ip_address = get_local_ip_address()
    dacirco_server = DaCircoRCPServer(
        vm_parameters=vm_parameters,
        minio_parameters=minio_parameters,
        local_ip_address=local_ip_address,
        event_log_file=event_log_file,
        sched_parameters=sched_parameters,
        pod_parameters=pod_parameters,
    )
    dacirco_pb2_grpc.add_DaCircogRPCServiceServicer_to_server(dacirco_server, server)
    listen_addr = "[::]:" + str(port)
    server.add_insecure_port(listen_addr)
    _logger.info("Starting server on %s", listen_addr)

    await server.start()

    # from https://github.com/grpc/grpc/pull/26622/files to avoid the need to
    # press Ctl-C twice to stop the server
    async def server_graceful_shutdown() -> None:
        logging.info("Starting graceful shutdown...")
        # Shuts down the server with 1 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(2)

    _cleanup_coroutines.append(server_graceful_shutdown())
    coroutines = [server.wait_for_termination(), dacirco_server.periodic_call()]
    await asyncio.gather(*coroutines, return_exceptions=False)


def run_server(
    vm_parameters: VMParameters,
    minio_parameters: MinIoParameters,
    port: int,
    ip_address: str,
    event_log_file: str,
    sched_parameters: SchedulerParameters,
    pod_parameters: PodParameters,
) -> None:
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            serve(
                vm_parameters=vm_parameters,
                minio_parameters=minio_parameters,
                port=port,
                ip_address=ip_address,
                event_log_file=event_log_file,
                sched_parameters=sched_parameters,
                pod_parameters=pod_parameters,
            )
        )
    finally:
        if _cleanup_coroutines:
            loop.run_until_complete(*_cleanup_coroutines)
        loop.close()
