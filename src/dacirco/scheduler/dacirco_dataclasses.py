from dataclasses import dataclass
from typing import Optional
from enum import Enum, auto


@dataclass
class TCRequestDesc:
    """
    Dataclass describing a transcoding request.

    It contains all the attributes given by the user (input video, bitrate,
    speed), as well a unique identifier that can be used as a correlation
    identifier.
    """

    input_video: str  # The name of the input video
    output_video: str  # The name of the output video
    bitrate: int  # The desired bitrate
    speed: str  # How fast should the transconding process be
    request_id: str  # The identifier of the corresponding client request


@dataclass
class TCTaskDesc:
    """
    Dataclass describing a transcoding task.

    A task represents a given scheduling choice: it contains the
    :class:`TCRequestDesc`, plus the details of the transconding worker that has
    been selected to execute the transcoding.  It also contains a unique
    identifier to guarantee its uniqueness.

    If there are no errors, each user request generates only one task.  If this
    task fails, the scheduler can generate another one to try to address the
    problem.

    When the :class:`scheduler <dacirco.scheduler.Scheduler>` wants to ask the
    gRPC Service to send a task to a given worker, it returns an instance of
    this class.  The gRPC service translates this into a TCTask message (see
    :doc:`gRPC messages <../proto>`).
    """

    request_desc: TCRequestDesc  # The request corresponding to this task
    worker_id: str  # The id of the worker for this task
    task_id: str  # The unique task identifier


@dataclass
class SchedRes:
    """Return type of the :class:`Scheduler` and :class:`TCWorkerHandler`
    methods called by the :class:`gRPC server
    <dacirco.grpc_service.server.DaCircoRCPServer>`

    Note: All the methods of the Scheduler class called by the gRPC server must
        return an instance of this class.

    """

    #: Whether the call was successful or not (set to False in the case of error).
    success: bool
    #: Contains the error message when `success` is `False`
    error_message: Optional[str] = None
    #: Contains an instance of :class:`TCTaskDesc <dacirco.scheduler.tc_task_desc.TCTaskDesc>`
    #: whenever the scheduler wants to instruct the gRPC server to send a transcoding task to
    #: a worker.
    tc_task_desc: Optional[TCTaskDesc] = None


@dataclass
class VMParameters:
    """Dataclass containing all the parameters needed to start an OpenStack VM."""

    image: str  # The name of the image to use
    flavor: str  # The name of tha flavor to use
    key: str  # The name OpenStack key to inject into the VM
    security_group: str  # The name of the security group for the VM
    network: str  # The name of the network to use
    concentrate_workers: bool  # If true, try to place all the workers on the same node
    spread_workers: bool  # If true, try to place workers on different nodes


class RunnerType(Enum):
    """Enum for the runner type."""

    VM = auto()
    POD = auto()


@dataclass
class MinIoParameters:
    """Dataclass containing all the parameters needed to connect to MinIO."""

    server: str  # The name (or IP address) of the server
    port: int  # The port number
    access_key: str  # The access key (username)
    access_secret: str  # The access secret (password)


class AlgorithmType(Enum):
    """Enum for the Algorithm type."""

    MINIMIZE_WORKERS = auto()
    GRACE_PERIOD = auto()


@dataclass
class SchedulerParameters:
    """Dataclass containing the parameters for the scheduler"""

    algorithm: AlgorithmType  # The scheduling algorithm
    max_workers: int  # The maximum number of workers
    call_interval: int  # How often (in seconds) the server calls the periodic function of the scheduler
    max_idle_time: int  # How many seconds before a worker is destroyed (grace period algorithm)
    runner_type: RunnerType  # The runner type


@dataclass
class PodParameters:
    """Dataclass containing all the parameters needed to start a Kubernetes pod."""

    image: str  # The name of the image to use
    cpu_request: str  # The vCPUs request of each pod
    memory_request: str  # The memory request for each pod
    cpu_limit: str  # The maximum number of vCPUs of each pod
    memory_limit: str  # The maximum memory of each pod
    node_selector: str  # The name of the node where pods must run
    concentrate_workers: bool  # If true, try to place all the workers on the same node
    spread_workers: bool  # If true, try to place workers on different nodes
