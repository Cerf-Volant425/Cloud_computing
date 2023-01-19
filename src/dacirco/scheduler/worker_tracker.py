from enum import Enum, auto
import logging
from dataclasses import dataclass
from typing import Optional
from dacirco.scheduler.dacirco_dataclasses import TCRequestDesc

_logger = logging.getLogger(__name__)


class WorkerStatus(Enum):
    BOOTING = auto()
    DOWNLOADING_FILE = auto()
    TRANSCODING = auto()
    UPLOADING_FILE = auto()
    IDLE = auto()
    STOPPED = auto()
    NOT_FOUND = auto()


# Called this class WorkerDescDC to avoid clashing with the WrokerDesc
# protobuf message.
@dataclass
class WorkerDescDC:
    name: str
    id: str
    cpus: str
    memory: str
    node: str


@dataclass
class _WorkerDescAndStatus:
    worker_desc: WorkerDescDC
    status: WorkerStatus


class WorkerTracker:
    """Class storing the state of each worker.

    The REST API need this information for the `/workers` and `/worker/{worker_id}` end
    points.

    The scheduler MUST call the :meth:`~WorkerTracker.set_status` method to
    update the status of each worker whenever needed.

    The scheduler tracks only active workers.  This class stores information about
    all the workers, including those that have been stopped.
    """

    def __init__(self) -> None:
        self._workers: dict[str, _WorkerDescAndStatus] = {}

    def add_worker(self, name: str, id: str, cpus: str, memory: str, node: str) -> None:
        """Adds a new request.

        Args:
            name (str): the name of the worker.
            id (str): the UUID of the worker.
            cpus (str): the number of vcpus of the worker.
            memory (str): the memory of the worker.
            node (str): the name of the node running the worker.
        """
        self._workers[id] = _WorkerDescAndStatus(
            worker_desc=WorkerDescDC(
                name=name, id=id, memory=memory, cpus=cpus, node=node
            ),
            status=WorkerStatus.BOOTING,
        )

    def set_worker_status(self, worker_id: str, status: WorkerStatus) -> None:
        """Sets the status of a worker.

        If the worker ID is unknown, it simply logs an error.

        Args:
            worker_id (str): the ID of the worker to update.
            status (WorkerStatus): the new status of the worker.
        """
        if worker_id in self._workers.keys():
            self._workers[worker_id].status = status
        else:
            _logger.error("Unknown worker id: %s", worker_id)

    def get_workers(self) -> list[str]:
        """Returns a list of all the worker IDs.

        Returns:
            list[str]: the list of all worker IDs.
        """
        return list(self._workers.keys())

    def get_worker(self, worker_id: str) -> Optional[WorkerDescDC]:
        """Returns the description of a given worker

        Args:
            worker_id (str): the ID of the worker.

        Returns:
            Optional[_WorkerDesc]: the worker description (None if not found)
        """
        if worker_id in self._workers.keys():
            return self._workers[worker_id].worker_desc
        else:
            return None

    def get_worker_status(self, request_id: str) -> WorkerStatus:
        """Returns the state of a request.

        Returns `RequestStatus.NOT_FOUND` if the request ID is unknown.

        Args:
            request_id (str): the ID of the request.

        Returns:
            RequestStatus: the status of the request.
        """
        if request_id in self._workers.keys():
            return self._workers[request_id].status
        else:
            return WorkerStatus.NOT_FOUND
