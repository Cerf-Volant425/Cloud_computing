import logging
import os
from collections import deque
from typing import Optional
from dataclasses import asdict
from datetime import timedelta


from dacirco.grpc_service.events import (
    TCWorkerErrorEvent,
    TCWorkerEvent,
    TCWorkerEventType,
)
from dacirco.scheduler.dacirco_dataclasses import (
    MinIoParameters,
    RunnerType,
    SchedRes,
    SchedulerParameters,
    AlgorithmType,
    TCRequestDesc,
    VMParameters,
    PodParameters,
)
from dacirco.scheduler.event_logger import EventLogger
from dacirco.scheduler.request_tracker import RequestStatus, RequestTracker
from dacirco.scheduler.vm_manager import VMManager
from dacirco.scheduler.pod_manager import PodManager
from dacirco.scheduler.worker_tracker import WorkerStatus, WorkerTracker, WorkerDescDC

_logger = logging.getLogger(__name__)
worker_1_id = ""


class Scheduler:
    def __init__(
        self,
        vm_parameters: VMParameters,
        pod_parameters: PodParameters,
        minio_parameters: MinIoParameters,
        local_ip_address: str,
        event_log_file: str,
        sched_parameters: SchedulerParameters,
    ) -> None:
        self._request_queue = deque[TCRequestDesc]()
        if event_log_file:
            self._EVENT_LOG_FILE_NAME = event_log_file
        else:
            self._EVENT_LOG_FILE_NAME = self._pick_event_log_file_name()
        self._event_logger = EventLogger(self._EVENT_LOG_FILE_NAME)
        self._request_tracker = RequestTracker()
        self._worker_tracker = WorkerTracker()
        self._algorithm = sched_parameters.algorithm
        self._max_idle_time = sched_parameters.max_idle_time
        if sched_parameters.runner_type is RunnerType.VM:
            self._runner_manager = VMManager(
                event_logger=self._event_logger,
                vm_parameters=vm_parameters,
                minio_parameters=minio_parameters,
                request_tracker=self._request_tracker,
                worker_tracker=self._worker_tracker,
                max_workers=sched_parameters.max_workers,
                local_ip_address=local_ip_address,
            )
            self._write_config_log_file(
                vm_parameters=vm_parameters,
                sched_parameters=sched_parameters,
                pod_parameters=pod_parameters,
            )
        elif sched_parameters.runner_type is RunnerType.POD:
            self._runner_manager = PodManager(
                event_logger=self._event_logger,
                pod_parameters=pod_parameters,
                minio_parameters=minio_parameters,
                request_tracker=self._request_tracker,
                worker_tracker=self._worker_tracker,
                max_workers=sched_parameters.max_workers,
                local_ip_address=local_ip_address,
            )
            self._write_config_log_file(
                vm_parameters=vm_parameters,
                sched_parameters=sched_parameters,
                pod_parameters=pod_parameters,
            )

    def _pick_event_log_file_name(self) -> str:
        """Finds the first unused log file name

        To avoid overwriting the same event log, this function looks for the
        first available name of the form `tc-events-01.txt`, `tc-events-02.txt`
        etc.  The base name is hard coded to `tc-events`.

        Raises: SystemExit: If there are already 99 files of this type, this
                            function causes the program to exit.

        Returns: str: The complete name for the event log file.
        """
        base_name = "tc-events"
        i = 1
        while os.path.exists(f"{base_name}-{i:02d}.txt"):
            i += 1
            if i == 100:
                _logger.error("Too many existing log files, giving up")
                raise SystemExit
        return f"{base_name}-{i:02d}.txt"

    def _write_config_log_file(
        self,
        vm_parameters: VMParameters,
        sched_parameters: SchedulerParameters,
        pod_parameters: PodParameters,
    ) -> None:
        _logger.debug("Writing file with current configuration")
        with open(self._EVENT_LOG_FILE_NAME[:-4] + ".config.txt", "w") as of:
            of.write(str(asdict(sched_parameters)) + "\n")
            of.write(str(asdict(vm_parameters)) + "\n")
            of.write(str(asdict(pod_parameters)))

    def new_request(
        self,
        input_video: str,
        output_video: str,
        bitrate: int,
        speed: str,
        request_id: str,
    ) -> SchedRes:
        q_size = len(self._request_queue)
        res = SchedRes(success=True)
        req = TCRequestDesc(
            input_video=input_video,
            output_video=output_video,
            bitrate=bitrate,
            speed=speed,
            request_id=request_id,
        )
        _logger.info(
            "Received new request. Queue size: %i, id: %s, bitrate: %i, speed: %s",
            q_size,
            input_video,
            bitrate,
            speed,
        )
        self._event_logger.request_received(req)
        worker = self._runner_manager.get_idle_worker()
        self._request_tracker.add_request(req)
        if worker:
            _logger.debug("Assigning request to worker %s", worker.name)
            tc_task_desc = worker.assign_tc_task(req)
            if tc_task_desc:
                res.tc_task_desc = tc_task_desc
        else:
            _logger.info("Enqueueing request (no available worker)")

            if req.speed[0] == "u":
                self._request_queue.appendleft(req)
            else:
                self._request_queue.append(req)
            # self._request_queue.appendleft(req)
        return res

    def register_worker(self, name: str, worker_id: str) -> SchedRes:
        res = SchedRes(success=True)
        worker = self._runner_manager.get_tc_worker(worker_id=worker_id)
        if not worker:
            msg = f"Received registration request from unknown worker {worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            if len(self._request_queue):
                global worker_1_id
                if not worker_1_id:
                    req = self._request_queue.pop()
                    if req.speed[0] == "u":
                        worker_1_id = worker.id
                else:
                    if worker.id != worker_1_id:
                        req = self._request_queue.pop()
                    else:
                        req = self._request_queue.popleft()

                _logger.info("Found enqueued request, video: %s", req.input_video)
                _logger.info(
                    "Assigning request to worker %s (%s)", worker.name, worker.id
                )
                tc_task_desc = worker.assign_tc_task(req)
                if tc_task_desc:
                    res.tc_task_desc = tc_task_desc
            elif self._algorithm is AlgorithmType.MINIMIZE_WORKERS:
                self._runner_manager.destroy_runner(worker_id=worker.id)
            worker = self._runner_manager.get_tc_worker(worker_id=worker_id)
            if worker:
                node, cpus, memory = self._runner_manager.get_runner_info(
                    worker_id=worker_id
                )
                self._event_logger.runner_registered(
                    name=name, id=worker_id, node=node, cpus=cpus, memory=memory
                )
                self._worker_tracker.set_worker_status(
                    worker_id=worker_id, status=WorkerStatus.IDLE
                )
                res = worker.handle_registration()
        return res

    def process_event(self, event: TCWorkerEvent) -> SchedRes:
        res = SchedRes(success=True)
        worker = self._runner_manager.get_tc_worker(worker_id=event.worker_id)
        if not worker:
            msg = f"Received event from unknown worker {event.worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            if event.type is TCWorkerEventType.FILE_DOWNLOADED:
                res = worker.handle_file_downloaded(event.task_id)
            elif event.type is TCWorkerEventType.TRANSCODING_COMPLETED:
                res = worker.handle_transcoding_completed(event.task_id)
            elif event.type is TCWorkerEventType.KEEPALIVE:
                worker.handle_keepalive(event.worker_id)
            elif event.type is TCWorkerEventType.FILE_UPLOADED:
                res = worker.handle_file_uploaded(event.task_id)
                if len(self._request_queue):
                    if event.worker_id != worker_1_id:
                        req = self._request_queue.pop()
                    else:
                        req = self._request_queue.popleft()
                    _logger.info("Found enqueued request, video: %s", req.input_video)
                    _logger.info(
                        "Assigning request to worker %s (%s)", worker.name, worker.id
                    )
                    tc_task_desc = worker.assign_tc_task(req)
                    if tc_task_desc:
                        res.tc_task_desc = tc_task_desc
                else:
                    _logger.info("No enqueued requests")
                    if self._algorithm is AlgorithmType.MINIMIZE_WORKERS:
                        self._runner_manager.destroy_runner(worker_id=worker.id)
        return res

    def process_error_event(self, event: TCWorkerErrorEvent) -> None:
        worker = self._runner_manager.get_tc_worker(worker_id=event.worker_id)
        if not worker:
            msg = f"Received an event from unknown worker {event.worker_id}"
            _logger.error(msg)
        else:
            worker.handle_error(event)
            self._runner_manager.destroy_runner(worker_id=event.worker_id)
            if len(self._request_queue):
                worker = self._runner_manager.get_idle_worker()
                if worker:
                    if event.worker_id != worker_1_id:
                        req = self._request_queue.pop()
                    else:
                        req = self._request_queue.popleft()
                    _logger.info("Found enqueued request, video: %s", req.input_video)
                    _logger.info(
                        "Assigning request to worker %s (%s)", worker.name, worker.id
                    )
                    tc_task_desc = worker.assign_tc_task(req)
                    if tc_task_desc:
                        _logger.error("Internal Error E643")

    def periodic_function(self) -> None:
        if self._algorithm is AlgorithmType.GRACE_PERIOD:
            self._runner_manager.destroy_idle_workers(
                timedelta(seconds=self._max_idle_time)
            )

    def get_requests(self) -> list[str]:
        return self._request_tracker.get_requests()

    def get_request(self, request_id: str) -> Optional[TCRequestDesc]:
        return self._request_tracker.get_request(request_id)

    def get_request_status(self, request_id: str) -> RequestStatus:
        return self._request_tracker.get_request_status(request_id)

    def get_workers(self) -> list[str]:
        return self._worker_tracker.get_workers()

    def get_worker(self, worker_id: str) -> Optional[WorkerDescDC]:
        return self._worker_tracker.get_worker(worker_id)

    def get_worker_status(self, worker_id: str) -> WorkerStatus:
        return self._worker_tracker.get_worker_status(worker_id)
