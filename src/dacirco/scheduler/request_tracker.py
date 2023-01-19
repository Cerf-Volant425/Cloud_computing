from enum import Enum, auto
import logging
from dataclasses import dataclass
from typing import Optional
from dacirco.scheduler.dacirco_dataclasses import TCRequestDesc

_logger = logging.getLogger(__name__)


class RequestStatus(Enum):
    WAITING = auto()
    STARTED = auto()
    COMPLETED = auto()
    ERROR = auto()
    NOT_FOUND = auto()


@dataclass
class _RequestAndStatus:
    request_desc: TCRequestDesc
    status: RequestStatus


class RequestTracker:
    """Class storing the state of each request.

    The REST API need this information for the `/jobs` and `/jobs/{jobId}` end
    points.

    The scheduler MUST call the :meth:`~RequestTracker.set_status` method to
    update the status of each request whenever needed.

    The scheduler tracks only active connections.  This class stores information
    for all requests, including completed ones (successfully or not).
    """

    def __init__(self) -> None:
        self._requests: dict[str, _RequestAndStatus] = {}

    def add_request(self, req: TCRequestDesc) -> None:
        """Adds a new request.

        Args:
            req (TCRequestDesc): the description of the new request.
        """
        self._requests[req.request_id] = _RequestAndStatus(req, RequestStatus.WAITING)

    def set_request_status(self, request_id: str, status: RequestStatus) -> None:
        """Sets the status of a request.

        If the request ID is unknown, it simply logs an error.

        Args:
            request_id (str): the ID of the request to update.
            status (RequestStatus): the new status of the request.
        """
        if request_id in self._requests.keys():
            self._requests[request_id].status = status
        else:
            _logger.error("Unknown request id: %s", request_id)

    def get_requests(self) -> list[str]:
        """Returns a list of all the request IDs.

        Returns:
            list[str]: the list of all request IDs.
        """
        return list(self._requests.keys())

    def get_request(self, request_id: str) -> Optional[TCRequestDesc]:
        """Returns the description of a given request

        Args:
            request_id (str): the ID of the request.

        Returns:
            Optional[TCRequestDesc]: the request description (None if not found)
        """
        if request_id in self._requests.keys():
            return self._requests[request_id].request_desc
        else:
            return None

    def get_request_status(self, request_id: str) -> RequestStatus:
        """Returns the state of a request.

        Returns `RequestStatus.NOT_FOUND` if the request ID is unknown.

        Args:
            request_id (str): the ID of the request.

        Returns:
            RequestStatus: the status of the request.
        """
        if request_id in self._requests.keys():
            return self._requests[request_id].status
        else:
            return RequestStatus.NOT_FOUND
