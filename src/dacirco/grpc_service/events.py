from enum import Enum, auto

from dacirco.proto.dacirco_pb2 import GrpcErrorEvent, GrpcEvent

"""The typing hints of the grpc objects are not easy to work with.  This is why
we have defined dedicated classes with clear type hints.
"""


class TCWorkerEventType(Enum):
    FILE_DOWNLOADED = auto()
    TRANSCODING_COMPLETED = auto()
    FILE_UPLOADED = auto()
    KEEPALIVE = auto()
    INVALID = auto()


class TCWorkerEvent:
    def __init__(self, grpc_event: GrpcEvent) -> None:
        self.worker_id = str(grpc_event.worker_id)
        self.task_id = str(grpc_event.task_id)
        self.type = TCWorkerEventType.INVALID
        if grpc_event.event_type is GrpcEvent.FILE_DOWNLOADED:
            self.type = TCWorkerEventType.FILE_DOWNLOADED
        elif grpc_event.event_type is GrpcEvent.TRANSCODING_COMPLETED:
            self.type = TCWorkerEventType.TRANSCODING_COMPLETED
        elif grpc_event.event_type is GrpcEvent.FILE_UPLOADED:
            self.type = TCWorkerEventType.FILE_UPLOADED
        elif grpc_event.event_type is GrpcEvent.KEEPALIVE:
            self.type = TCWorkerEventType.KEEPALIVE


class TCWorkerErrorEventType(Enum):
    INVALID = auto()
    TRANSCODING_FAILED = auto()
    STORAGE_ERROR = auto()


class TCWorkerErrorEvent:
    def __init__(self, grpc_error_event: GrpcErrorEvent) -> None:
        self.worker_id = str(grpc_error_event.worker_id)
        self.task_id = str(grpc_error_event.task_id)
        self.error_message = str(grpc_error_event.error_message)
        self.type = TCWorkerErrorEventType.INVALID
        if grpc_error_event.error_type is GrpcErrorEvent.STORAGE_ERROR:
            self.type = TCWorkerErrorEventType.STORAGE_ERROR
        elif grpc_error_event.error_type is GrpcErrorEvent.TRANSCODING_FAILED:
            self.type = TCWorkerErrorEventType.TRANSCODING_FAILED
