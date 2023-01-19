import logging
import uuid
from collections import deque
from enum import Enum, auto
from typing import Optional
from datetime import date, datetime, time, timedelta

import openstack  # type: ignore
from dacirco.grpc_service.events import TCWorkerErrorEvent, TCWorkerErrorEventType
from dacirco.scheduler.dacirco_dataclasses import SchedRes, TCRequestDesc, TCTaskDesc
from dacirco.scheduler.event_logger import EventLogger
from dacirco.scheduler.request_tracker import RequestStatus, RequestTracker
from dacirco.scheduler.worker_tracker import WorkerTracker, WorkerStatus

_logger = logging.getLogger(__name__)


class TCWorkerStatus(Enum):
    """
    Constants representing the four possible states of a transcoding worker.

    BOOTING: the controller has crated a new worker but it has not yet received
    the corresponding registration.

    STARTING_JOB: the controller has sent a transcoding job to the worker but it
    has not yet received the "task started" message.

    BUSY: the controller has received the "file copied" message, therefore the
    worker is currently working.
    """

    BOOTING = auto()
    DOWNLOADING_FILE = auto()
    TRANSCODING = auto()
    UPLOADING_FILE = auto()
    IDLE = auto()


class TCWorkerHandler:
    """
    This class keeps track of the state of a single transcoding worker.  In
    other words, there is exactly one instance of this class for each existing
    worker.

    The four possible states are represented by the :class:`TCWorkerStatus`.
    """

    def __init__(
        self,
        name: str,
        id: str,
        event_logger: EventLogger,
        request_tracker: RequestTracker,
        worker_tracker: WorkerTracker,
    ) -> None:
        self.id = id
        self.name = name
        self.task_queue = deque[TCTaskDesc]()
        self.current_tc_task: Optional[TCTaskDesc] = None
        self.status = TCWorkerStatus.BOOTING
        self._event_logger = event_logger
        self._request_tracker = request_tracker
        self._worker_tracker = worker_tracker
        self._beginning_idle_time: Optional[datetime] = None

    def assign_tc_task(self, req: TCRequestDesc) -> Optional[TCTaskDesc]:
        """Assigns a transcoding task to a worker.

        This method adds the task to the queue for this worker.  It cannot fail
        as the queue has no upper limit.  If the worker is idle, it calls
        :func:`~TCWorkerHandler.start_tc_task` method in case the task that we
        just added to the queue was the only pending task.  In this case, the
        `start_tc_task` method returns a
        :class:`~dacirco.scheduler.tc_task_desc.TCTaskDesc` object that will be
        returned to the gRPC server that will finally send a message to the
        worker instructing it to start the transcoding job.

        Args: req (TCRequestData): The request to enqueue

        Returns: Optional[TCTaskDesc]: The task description to return to the
        scheduler if the worker is idle.
        """

        _logger.debug(
            "Assigning TC task (video %s) to worker: %s (%s)",
            req.input_video,
            self.name,
            self.id,
        )
        res = None
        tc_task = TCTaskDesc(
            request_desc=req, worker_id=self.id, task_id=str(uuid.uuid4())
        )
        self.task_queue.appendleft(tc_task)
        self._event_logger.task_assigned(tc_task)
        if self._check_queue():
            res = self.current_tc_task
        return res

    def _check_queue(self) -> bool:
        """Check if there is a task waiting in the queue.

        If this worker is idle and the queue is not empty, remove one task and
        assign it to `self.current_tc_task`, and return True so that the caller
        will inform the gRPC server to send the appropriate message to the
        worker.

        Returns: bool: Whether a task was removed from the queue or not.
        """
        if self.task_queue and self.status == TCWorkerStatus.IDLE:
            tc_task = self.task_queue.pop()
            self.current_tc_task = tc_task
            self.status = TCWorkerStatus.DOWNLOADING_FILE
            _logger.debug(
                "Starting tc task. Worker: %s (%s), video: %s, task: %s",
                self.name,
                self.id,
                tc_task.request_desc.input_video,
                tc_task.task_id,
            )
            self._event_logger.task_started(tc_task=tc_task)
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.DOWNLOADING_FILE
            )

            return True
        else:
            return False

    def handle_registration(self) -> SchedRes:
        """Handles the registration of the corresponding worker.

        First, it checks if the current state is `BOOTING` as this is tne only
        state where we can receive the registration request.  Then, if the state
        is indeed `BOOTING`, it checks if there is a pending task in the queue,
        if this is the case, it sets the `tc_task_desc` of the return value, so
        that the caller (the scheduler) will inform the gRPC server to send the
        appropriate message to this worker.

        Returns: SchedRes: The result of this operation (success/failure and
        :class:`~dacirco.scheduler.dataclasses.TCTaskDesc` if a new task must be
        started)
        """
        res = self._check_state(TCWorkerStatus.BOOTING)
        if res.success:
            self.status = TCWorkerStatus.IDLE
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.IDLE
            )
            self._beginning_idle_time = datetime.now()
            t = datetime.now()
            if self._check_queue():
                res.tc_task_desc = self.current_tc_task
        return res

    def handle_file_downloaded(self, task_id: str) -> SchedRes:
        res_check_state = self._check_state(TCWorkerStatus.DOWNLOADING_FILE)
        res_check_task_id = self._check_task_id(task_id=task_id)
        res = SchedRes(success=True)
        if res_check_state.success and res_check_task_id and self.current_tc_task:
            self.status = TCWorkerStatus.TRANSCODING
            self._event_logger.file_downloaded(self.current_tc_task)
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.TRANSCODING
            )
            self._request_tracker.set_request_status(
                self.current_tc_task.request_desc.request_id, RequestStatus.STARTED
            )
        else:
            if not res_check_state.success:
                res = res_check_state
            if not res_check_task_id.success:
                res = res_check_task_id
        return res

    def handle_transcoding_completed(self, task_id: str) -> SchedRes:
        res_check_state = self._check_state(TCWorkerStatus.TRANSCODING)
        res_check_task_id = self._check_task_id(task_id=task_id)
        res = SchedRes(success=True)
        if res_check_state.success and res_check_task_id and self.current_tc_task:
            self.status = TCWorkerStatus.UPLOADING_FILE
            self._event_logger.transcoding_completed(self.current_tc_task)
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.UPLOADING_FILE
            )
        else:
            if not res_check_state.success:
                res = res_check_state
            if not res_check_task_id.success:
                res = res_check_task_id
        return res

    def handle_file_uploaded(self, task_id: str) -> SchedRes:
        res_check_state = self._check_state(TCWorkerStatus.UPLOADING_FILE)
        res_check_task_id = self._check_task_id(task_id=task_id)
        res = SchedRes(success=True)
        if res_check_state.success and res_check_task_id and self.current_tc_task:
            self._event_logger.file_uploaded(self.current_tc_task)
            self._request_tracker.set_request_status(
                self.current_tc_task.request_desc.request_id, RequestStatus.COMPLETED
            )
            self.status = TCWorkerStatus.IDLE
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.IDLE
            )
            self._beginning_idle_time = datetime.now()
            self.current_tc_task = None
            if self._check_queue():
                res.tc_task_desc = self.current_tc_task
        else:
            if not res_check_state.success:
                res = res_check_state
            if not res_check_task_id.success:
                res = res_check_task_id
        return res

    def get_idle_time(self) -> timedelta:
        if self.status is TCWorkerStatus.IDLE and self._beginning_idle_time:
            return datetime.now() - self._beginning_idle_time
        else:
            return timedelta(0)

    def handle_keepalive(self, worker_id: str) -> None:
        pass

    def handle_error(self, event: TCWorkerErrorEvent) -> SchedRes:
        res_check_task_id = self._check_task_id(task_id=event.task_id)
        if res_check_task_id.success and self.current_tc_task:
            self._request_tracker.set_request_status(
                self.current_tc_task.request_desc.request_id, RequestStatus.ERROR
            )
            if event.type is TCWorkerErrorEventType.TRANSCODING_FAILED:
                self._event_logger.transcoding_error(
                    event, self.current_tc_task.request_desc.request_id
                )
            elif event.type is TCWorkerErrorEventType.STORAGE_ERROR:
                self._event_logger.storage_error(
                    event, self.current_tc_task.request_desc.request_id
                )
            else:
                error_msg = f"Received unknown error type: {event.type}"
                _logger.error(f"Received unknown error type: %s", event.type)
                res_check_task_id.success = False
                res_check_task_id.error_message = error_msg
            self.status = TCWorkerStatus.IDLE
            self._worker_tracker.set_worker_status(
                worker_id=self.id, status=WorkerStatus.IDLE
            )
            self._beginning_idle_time = datetime.now()
        return res_check_task_id

    def _check_state(self, expected: TCWorkerStatus) -> SchedRes:
        res = SchedRes(success=True)
        if self.status != expected:
            error_message = (
                f"Received message from worker {self.name} "
                f"whose status was: {self.status} (rather than {expected} as expected)"
            )
            _logger.error(error_message)
            res.success = False
            res.error_message = error_message
        return res

    def _check_task_id(self, task_id: str) -> SchedRes:
        res = SchedRes(success=True)
        if self.current_tc_task:
            if task_id != self.current_tc_task.task_id:
                msg = (
                    f"Received message for task_id: {task_id} "
                    f"but the current_tc_task_data task id is: {self.current_tc_task.task_id}"
                )
                _logger.error(msg)
                res.success = False
                res.error_message = msg
        else:
            msg = (
                f"Received message for task_id: {task_id} "
                f"but the current_tc_task_data is None"
            )
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        return res
