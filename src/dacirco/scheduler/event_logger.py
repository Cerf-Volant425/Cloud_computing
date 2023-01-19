import logging
from datetime import datetime
from enum import Enum
from json import dumps

from dacirco.grpc_service.events import TCWorkerErrorEvent
from dacirco.scheduler.dacirco_dataclasses import TCRequestDesc, TCTaskDesc

_logger = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}  # type:ignore

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class EventCodes(str, Enum):
    REQUEST_RECEIVED = "EV1"
    RUNNER_CREATED = "EV2"
    TASK_ASSIGNED = "EV3"
    TASK_STARTED = "EV4"
    FILE_DOWNLOADED = "EV5"
    TRANSCODING_COMPLETED = "EV6"
    FILE_UPLOADED = "EV7"
    RUNNER_DESTROYED = "EV8"
    TRANSCODING_ERROR = "EV9"
    STORAGE_ERROR = "EV10"
    RUNNER_REGISTERED = "EV11"


class EventLogger(metaclass=Singleton):
    def __init__(self, file_name: str) -> None:
        self.file = open(file_name, "w")
        _logger.debug("Opened event file: %s", file_name)
        self.event_desc: dict[EventCodes, str] = {
            EventCodes.REQUEST_RECEIVED: "Request received",
            EventCodes.RUNNER_CREATED: "Runner created",
            EventCodes.TASK_ASSIGNED: "Task assigned",
            EventCodes.TASK_STARTED: "Task started",
            EventCodes.FILE_DOWNLOADED: "File dowloaded",
            EventCodes.TRANSCODING_COMPLETED: "Transcoding completed",
            EventCodes.FILE_UPLOADED: "File uploaded",
            EventCodes.RUNNER_DESTROYED: "Runner destroyed",
            EventCodes.TRANSCODING_ERROR: "Transcoding error",
            EventCodes.STORAGE_ERROR: "Storage error",
            EventCodes.RUNNER_REGISTERED: "Runner registered",
        }

    def __del__(self) -> None:
        self.file.close()

    def _create_common_fields(self, ec: EventCodes) -> dict[str, str]:
        d: dict[str, str] = {}
        d["time"] = str(datetime.now())
        d["code"] = str(ec.value)
        d["description"] = str(self.event_desc[ec])
        return d

    def _tc_request_desc_to_dict(self, req: TCRequestDesc) -> dict[str, str]:
        d: dict[str, str] = {}
        d["request_id"] = str(req.request_id)
        d["input_video"] = str(req.input_video)
        d["bitrate"] = str(req.bitrate)
        d["speed"] = str(req.speed)
        return d

    def _tc_task_desc_to_dict(self, tc_task: TCTaskDesc) -> dict[str, str]:
        d: dict[str, str] = {}
        d["request_id"] = str(tc_task.request_desc.request_id)
        d["worker_id"] = str(tc_task.worker_id)
        d["task_id"] = str(tc_task.task_id)
        return d

    def _error_event_to_dict(self, error_event: TCWorkerErrorEvent) -> dict[str, str]:
        d: dict[str, str] = {}
        d["worker_id"] = error_event.worker_id
        d["task_id"] = error_event.task_id
        d["error_message"] = error_event.error_message
        return d

    def _write_event(self, d: dict[str, str]) -> None:
        self.file.write(dumps(d) + "\n")

    def request_received(self, request: TCRequestDesc) -> None:
        ec = EventCodes.REQUEST_RECEIVED
        self._write_event(
            self._create_common_fields(ec) | self._tc_request_desc_to_dict(request),
        )

    def runner_created(self, name: str, id: str) -> None:
        ec = EventCodes.RUNNER_CREATED
        d: dict[str, str] = {}
        d["name"] = name
        d["worker_id"] = id
        self._write_event(self._create_common_fields(ec) | d)

    def runner_registered(
        self, name: str, id: str, node: str, cpus: str, memory: str
    ) -> None:
        ec = EventCodes.RUNNER_REGISTERED
        d: dict[str, str] = {}
        d["name"] = name
        d["worker_id"] = id
        d["node"] = node
        d["cpus"] = cpus
        d["memory"] = memory
        self._write_event(self._create_common_fields(ec) | d)

    def task_assigned(self, tc_task: TCTaskDesc) -> None:
        ec = EventCodes.TASK_ASSIGNED
        self._write_event(
            self._create_common_fields(ec) | self._tc_task_desc_to_dict(tc_task)
        )

    def task_started(self, tc_task: TCTaskDesc) -> None:
        ec = EventCodes.TASK_STARTED
        self._write_event(
            self._create_common_fields(ec) | self._tc_task_desc_to_dict(tc_task)
        )

    def file_downloaded(self, tc_task: TCTaskDesc) -> None:
        ec = EventCodes.FILE_DOWNLOADED
        self._write_event(
            self._create_common_fields(ec) | self._tc_task_desc_to_dict(tc_task)
        )

    def transcoding_completed(self, tc_task: TCTaskDesc) -> None:
        ec = EventCodes.TRANSCODING_COMPLETED
        self._write_event(
            self._create_common_fields(ec) | self._tc_task_desc_to_dict(tc_task)
        )

    def file_uploaded(self, tc_task: TCTaskDesc) -> None:
        ec = EventCodes.FILE_UPLOADED
        self._write_event(
            self._create_common_fields(ec) | self._tc_task_desc_to_dict(tc_task)
        )

    def runner_destroyed(self, name: str, id: str) -> None:
        ec = EventCodes.RUNNER_DESTROYED
        d: dict[str, str] = {}
        d["name"] = name
        d["worker_id"] = id
        self._write_event(self._create_common_fields(ec) | d)

    def transcoding_error(
        self, error_event: TCWorkerErrorEvent, request_id: str
    ) -> None:
        ec = EventCodes.TRANSCODING_ERROR
        self._write_event(
            self._create_common_fields(ec) | self._error_event_to_dict(error_event)
        )

    def storage_error(self, error_event: TCWorkerErrorEvent, request_id: str) -> None:
        ec = EventCodes.STORAGE_ERROR
        self._write_event(
            self._create_common_fields(ec) | self._error_event_to_dict(error_event)
        )
