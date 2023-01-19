import asyncio
import logging
import sys
from typing import Any, Optional

import click
import grpc
from dacirco.proto.dacirco_pb2 import (
    GrpcErrorEvent,
    GrpcEvent,
    TCTask,
    WorkerDesc,
    gRPCServiceReply,
)
from dacirco.proto.dacirco_pb2_grpc import DaCircogRPCServiceStub
from minio import Minio  # type: ignore

_logger = logging.getLogger(__name__)


class TCWorker:
    """Implementation of the transcoding worker running on the VMs

    The :meth:`~TCWorker:run` method of this class spawns five tasks:
        1) :meth:`~TCWorker:get_tc_tasks` that blocks on receiving the tasks
        from the controller.
        2) :meth:`~TCWorker:download_file` that downloads the input file from MinIO.
        3) :meth:`~TCWorker:run_ffmpeg` that runs ffmpeg in a separate process.
        4) :meth:`~TCWorker:upload_file` that uploads the output file to MinIO.
        5) :meth:`~TCWorker:send_keep_alive` that sends the periodic keepalive messages.

    These tasks communicate thanks to three queues:
        1) `file_download_task_queue` between `get_tc_tasks` and `download_file`.
        2) `ffmpeg_task_queue` between `download_file` and `run_ffmpeg`.
        3) `file_upload_task_queue` between  `run_ffmpeg` and `upload_file`.
    """

    def __init__(
        self,
        name: str,
        id: str,
        keepalive_interval: int,
        grcp_server: str,
        grcp_port: str,
        minio_server: str,
        minio_port: str,
        minio_access_key: str,
        minio_secret_key: str,
        bucket_name: str,
        input_path: str,
        output_path: str,
    ) -> None:
        self.name = name
        self.id = id
        self.registration_done = False
        self.keepalive_interval = keepalive_interval
        self.grpc_server = grcp_server
        self.grpc_port = grcp_port
        self.bucket_name = bucket_name
        self.input_path = input_path
        self.output_path = output_path
        self.file_download_task_queue = asyncio.Queue[TCTask]()
        self.ffmpeg_task_queue = asyncio.Queue[TCTask]()
        self.file_upload_task_queue = asyncio.Queue[TCTask]()
        self.stub: Optional[DaCircogRPCServiceStub] = None
        if not minio_secret_key or not minio_access_key:
            _logger.fatal("You must specify both the minio access and secret keys")
            sys.exit()
        self.minio_client = Minio(
            f"{minio_server}:{minio_port}",
            access_key=minio_access_key,
            secret_key=minio_secret_key,
            secure=False,
        )
        _logger.debug(
            f"minio server: {minio_server}:{minio_port} access_key: {minio_access_key}, secret: {minio_secret_key}"
        )

    async def register_worker(self) -> None:
        if self.stub:
            response: gRPCServiceReply = await self.stub.register_worker(
                WorkerDesc(name=self.name, id=self.id)
            )
            _logger.info("Registration response success: %s", response.success)
            self.registration_done = True
        else:
            _logger.error("register_worker called with stub still set to None")

    async def get_tc_tasks(self) -> None:
        if self.stub:
            async for tc_task in self.stub.get_tasks(
                WorkerDesc(name=self.name, id=self.id)
            ):
                _logger.info(
                    "get tc task Received new transcoding task. Video: %s, rate: %s, speed: %s",
                    tc_task.input_video,
                    tc_task.bitrate,
                    tc_task.speed,
                )
                self.file_download_task_queue.put_nowait(tc_task)
                _logger.debug("request enqueued")
        else:
            _logger.error("get_tc_tasks called with stub still set to None")

    async def download_file(self) -> None:
        while True:
            tc_task = await self.file_download_task_queue.get()
            loop = asyncio.get_running_loop()
            download_succeeded = True
            storage_exception_desc = ""
            try:
                await loop.run_in_executor(
                    None,
                    self.minio_client.fget_object,
                    self.bucket_name,
                    f"{self.input_path}/{tc_task.input_video}",
                    "/tmp/input-file",
                )
            except Exception as exc:
                _logger.warn("Exception while downloading the input file: %s", exc)
                download_succeeded = False
                storage_exception_desc = str(exc)
                if self.stub:
                    await self.stub.submit_error(
                        GrpcErrorEvent(
                            error_type=GrpcErrorEvent.STORAGE_ERROR,
                            error_message=storage_exception_desc,
                            worker_id=self.id,
                            task_id=tc_task.task_id,
                        )
                    )

            _logger.debug("Done downloading the file")
            if download_succeeded:
                self.ffmpeg_task_queue.put_nowait(tc_task)
                if self.stub:
                    await self.stub.submit_event(
                        GrpcEvent(
                            event_type=GrpcEvent.FILE_DOWNLOADED,
                            worker_id=self.id,
                            task_id=tc_task.task_id,
                        )
                    )

    async def upload_file(self) -> None:
        while True:
            tc_task = await self.file_upload_task_queue.get()
            loop = asyncio.get_running_loop()
            upload_succeeded = True
            storage_exception_desc = ""
            try:
                await loop.run_in_executor(
                    None,
                    self.minio_client.fput_object,
                    self.bucket_name,
                    f"{self.output_path}/{tc_task.output_video}",
                    "/tmp/output-file",
                )
            except Exception as exc:
                _logger.warn("Exception while uploading output file: %s", exc)
                upload_succeeded = False
                storage_exception_desc = str(exc)
                if self.stub:
                    await self.stub.submit_error(
                        GrpcErrorEvent(
                            error_type=GrpcErrorEvent.STORAGE_ERROR,
                            error_message=storage_exception_desc,
                            worker_id=self.id,
                            task_id=tc_task.task_id,
                        )
                    )

            if upload_succeeded:
                _logger.debug("Done uploading the file")
                if self.stub:
                    await self.stub.submit_event(
                        GrpcEvent(
                            event_type=GrpcEvent.FILE_UPLOADED,
                            worker_id=self.id,
                            task_id=tc_task.task_id,
                        )
                    )

    async def send_keep_alive(self) -> None:
        await asyncio.sleep(1)
        while True:
            if self.stub and self.registration_done:
                await self.stub.submit_event(
                    GrpcEvent(
                        event_type=GrpcEvent.KEEPALIVE,
                        worker_id=self.id,
                        task_id="",
                    )
                )
            await asyncio.sleep(self.keepalive_interval)

    async def run_ffmpeg(self) -> None:
        while True:
            tc_task = await self.ffmpeg_task_queue.get()
            _logger.debug("run_ffmpeg received new tc request")
            args = [
                "/usr/bin/ffmpeg",
                "-y",
                "-hide_banner",
                "-v",
                # "fatal",
                "error",
                "-i",
                "/tmp/input-file",
                "-c:v",
                "libx265",
                "-b:v",
                str(tc_task.bitrate),
                "-preset",
                tc_task.speed,
                "-max_muxing_queue_size",
                "1024",
                "-x265-params",
                "--log-level=1",
                "-f",
                "mp4",
                "/tmp/output-file",
            ]
            if self.stub:
                _logger.debug(
                    "About to start transcoding. Video: %s", tc_task.input_video
                )
                ffmpeg_proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _logger.info("Started task: %s", tc_task.task_id)
                ffmpeg_stdout, ffmpeg_stderr = await ffmpeg_proc.communicate()
                ffmpeg_ret_code = (
                    ffmpeg_proc.returncode
                    if (ffmpeg_proc.returncode is not None)
                    else -1
                )
                _logger.info("====ffmpeg retcode: %i", ffmpeg_ret_code)
                _logger.debug("ffmpeg stdout: \n%s", ffmpeg_stdout.decode())
                _logger.debug("ffmpeg stderr: \n%s", ffmpeg_stderr.decode())

                _logger.debug(
                    "Sent trasncoding completed. Worker: %s, video: %s, task: %s.",
                    self.id,
                    tc_task.input_video,
                    tc_task.task_id,
                )
                if ffmpeg_ret_code == 0:
                    await self.stub.submit_event(
                        GrpcEvent(
                            event_type=GrpcEvent.TRANSCODING_COMPLETED,
                            worker_id=self.id,
                            task_id=tc_task.task_id,
                        )
                    )
                    self.file_upload_task_queue.put_nowait(tc_task)
                else:
                    if self.stub:
                        await self.stub.submit_error(
                            GrpcErrorEvent(
                                error_type=GrpcErrorEvent.TRANSCODING_FAILED,
                                error_message=f"ffmpeg exit code: {ffmpeg_ret_code}",
                                worker_id=self.id,
                                task_id=tc_task.task_id,
                            )
                        )
            else:
                _logger.error("run_ffmpeg called with stub still set to None")

    async def run(self):
        async with grpc.aio.insecure_channel(  # type: ignore
            self.grpc_server + ":" + str(self.grpc_port)
        ) as channel:
            self.stub = DaCircogRPCServiceStub(channel)
            await self.register_worker()
            coroutines = [
                self.get_tc_tasks(),
                self.run_ffmpeg(),
                self.download_file(),
                self.upload_file(),
                self.send_keep_alive(),
            ]
            res = await asyncio.gather(*coroutines, return_exceptions=False)
            return res


@click.command()
@click.option("--name", default="vm-0")
@click.option("--id", default="invalid")
@click.option("--keepalive-interval", default=15, type=int)
@click.option("--grpc-server", default="localhost")
@click.option("--grpc-port", default=50051, type=int)
@click.option("--minio-server", default="localhost")
@click.option("--minio-port", default="9000")
@click.option("--minio-access-key", type=str)
@click.option("--minio-secret-key", type=str)
@click.option("--log-file", type=str)
@click.option("--bucket-name", default="videos")
@click.option("--input-path", default="input-videos")
@click.option("--output-path", default="output-videos")
def tc_worker(
    name: str,
    id: str,
    keepalive_interval: int,
    grpc_server: str,
    grpc_port: str,
    minio_server: str,
    minio_port: str,
    minio_access_key: str,
    minio_secret_key: str,
    log_file: str,
    bucket_name: str,
    input_path: str,
    output_path: str,
) -> None:
    _logger.info("Starting TC Worker, name: %s, id: %s", name, id)
    _logger.info("Server: %s, port: %s", grpc_server, grpc_port)
    if log_file:
        _logger.info("Using log file: %s", log_file)
        logger = logging.getLogger("")
        logger.setLevel(logging.DEBUG)
        logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        formatter = logging.Formatter(logformat, datefmt="%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(log_file, mode="w")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)

    tc_worker = TCWorker(
        name=name,
        id=id,
        keepalive_interval=keepalive_interval,
        grcp_server=grpc_server,
        grcp_port=grpc_port,
        minio_server=minio_server,
        minio_port=minio_port,
        minio_access_key=minio_access_key,
        minio_secret_key=minio_secret_key,
        bucket_name=bucket_name,
        input_path=input_path,
        output_path=output_path,
    )
    asyncio.get_event_loop().run_until_complete(tc_worker.run())


def start_tc_worker() -> None:
    logging.basicConfig(level=logging.DEBUG)
    _logger.setLevel(logging.DEBUG)
    tc_worker(auto_envvar_prefix="TCWORKER")


if __name__ == "__main__":
    start_tc_worker()
