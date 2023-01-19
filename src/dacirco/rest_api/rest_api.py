#!/usr/bin/env python3
import logging
import sys
import click
import grpc
import uvicorn
from dacirco.proto.dacirco_pb2 import (
    TCRequest,
    TCRequestReply,
    Empty,
    RequestIDList,
    RequestID,
    TCRequestStatus,
    WorkerID,
    WorkerIDList,
    WorkerFullDesc,
    WorkerGrpcState,
)
from dacirco.proto.dacirco_pb2_grpc import DaCircogRPCServiceStub
from dacirco.rest_api.rest_api_schema import (
    Job,
    RequestState,
    RequestIDsListRest,
    WorkerIDsListRest,
    Worker,
    WorkerState,
)
from fastapi import Body, FastAPI, Request, Response, HTTPException
from pydantic import BaseModel

_logger = logging.getLogger(__name__)


app = FastAPI(title="Da Circo API ")
g_grpc_server = ""
g_grpc_port = ""


@app.get("/jobs", response_model=RequestIDsListRest, status_code=200)
async def get_jobs():
    """Handler for the GET /jobs request.

    Returns the list of all active and terminated jobs.
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: RequestIDList = stub.get_requests(Empty())
    return {"items": list(grpc_response.request_ids), "has_more": False}


@app.get("/jobs/{jobId}", response_model=Job, status_code=200)
async def get_job(jobId: str):
    """Handler for the GET /jobs/{jobId} request.

    Returns information about job #jobId

    :return: the description of the job

    :param jobId: the ID of the job
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: TCRequest = stub.get_request(RequestID(request_id=jobId))
    if grpc_response.input_video:
        res = Job(
            id_video=grpc_response.input_video,
            bitrate=grpc_response.bitrate,
            speed=grpc_response.speed,
        )
    else:
        res = {}
        raise HTTPException(status_code=404, detail="Request does not exist")

    return res


@app.get("/jobs/{jobId}/state")
async def get_job_state(jobId: str):
    """Handler for the GET /jobs/{jobID}/state request.

    Returns the state of job #jobId

    :return: the state of the job

    :param jobId: the ID of the job
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: TCRequestStatus = stub.get_request_status(
            RequestID(request_id=jobId)
        )
    res = RequestState.ERROR
    if grpc_response.request_status is TCRequestStatus.NOT_FOUND:
        raise HTTPException(status_code=404, detail="Request does not exist")
    elif grpc_response.request_status is TCRequestStatus.WAITING:
        res = RequestState.WAITING
    elif grpc_response.request_status is TCRequestStatus.STARTED:
        res = RequestState.STARTED
    elif grpc_response.request_status is TCRequestStatus.COMPLETED:
        res = RequestState.COMPLETED
    elif grpc_response.request_status is TCRequestStatus.ERROR:
        res = RequestState.ERROR
    else:
        raise HTTPException(status_code=503, detail="Internal server error")
    return res


@app.post("/jobs", status_code=201)
async def create_job(
    request: Request,
    response: Response,
    job: Job = Body(
        ...,
        example={
            "id_video": "bbb_0.mp4",
            "bitrate": 7000,
            "speed": "ultrafast",
        },
    ),
):
    """Handler for the POST /jobs/{"id_video", "bitrate", "speed"} request.

    Ask Da Circo Job Manager to create a new job to handle this request.
    Returns the Id of the created job.

    :param request: the received request
    :param response: the answer sent in response of the request
    :param job: the parameters of the job to create
    """
    _logger.debug("New job request, video: %s, ", job.id_video)
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: TCRequestReply = stub.submit_request(
            TCRequest(
                input_video=job.id_video,
                bitrate=job.bitrate,
                speed=job.speed,
                output_video="out-" + job.id_video,
            )
        )
        _logger.debug(
            "Grpc response: %s, request_id: %s",
            str(grpc_response.success),
            grpc_response.request_id,
        )
    resp = ["http://"]
    resp.append(f"{request.url.hostname}:{request.url.port}")
    resp.append("jobs")
    resp.append(grpc_response.request_id)
    response.headers["Location"] = "/".join(resp)
    return grpc_response.request_id


@app.get("/workers", response_model=WorkerIDsListRest, status_code=200)
async def get_workers():
    """Handler for the GET /workers request.

    Returns the list of all active and stopped workers.
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: WorkerIDList = stub.get_workers(Empty())
    return {"items": list(grpc_response.worker_ids), "has_more": False}


@app.get("/workers/{workerId}", response_model=Worker, status_code=200)
async def get_worker(workerId: str):
    """Handler for the GET /workers/{workerId} request.

    Returns information about worker #workerId

    :return: the description of the worker

    :param workerId: the ID of the worker
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: WorkerFullDesc = stub.get_worker(WorkerID(worker_id=workerId))
    if grpc_response.name:
        res = Worker(
            name=grpc_response.name,
            id=grpc_response.id,
            cpus=grpc_response.cpus,
            memory=grpc_response.memory,
            node=grpc_response.node,
        )
    else:
        res = {}
        raise HTTPException(status_code=404, detail="Worker does not exist")

    return res


@app.get("/workers/{workerId}/state")
async def get_worker_state(workerId: str):
    """Handler for the GET /workers/{workerId}/state request.

    Returns the state of the worker with id #workerId

    :return: the state of the worker

    :param workerId: the ID of the worker
    """
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        grpc_response: WorkerGrpcState = stub.get_worker_status(
            WorkerID(worker_id=workerId)
        )
    res = RequestState.ERROR
    if grpc_response.worker_status is WorkerGrpcState.NOT_FOUND:
        raise HTTPException(status_code=404, detail="Worker does not exist")
    elif grpc_response.worker_status is WorkerGrpcState.BOOTING:
        res = WorkerState.BOOTING
    elif grpc_response.worker_status is WorkerGrpcState.DOWNLOADING_FILE:
        res = WorkerState.DOWNLOADING_FILE
    elif grpc_response.worker_status is WorkerGrpcState.TRANSCODING:
        res = WorkerState.TRANSCODING
    elif grpc_response.worker_status is WorkerGrpcState.UPLOADING_FILE:
        res = WorkerState.UPLOADING_FILE
    elif grpc_response.worker_status is WorkerGrpcState.IDLE:
        res = WorkerState.IDLE
    elif grpc_response.worker_status is WorkerGrpcState.STOPPED:
        res = WorkerState.STOPPED
    else:
        raise HTTPException(status_code=503, detail="Internal server error")
    return res


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity")
@click.option(
    "-p",
    "--port",
    type=int,
    default=8000,
    show_default=True,
    help="The port number to bind to",
)
@click.option(
    "--grpc-server",
    default="localhost",
    show_default=True,
    help="The name (or IP address) of the gRPC server",
)
@click.option(
    "--grpc-port",
    default="50051",
    show_default=True,
    help="The port number of the gRPC server",
)
def run_rest_api(verbose: int, port: int, grpc_server: str, grpc_port: int):
    loglevel = logging.WARN
    if verbose == 1:
        loglevel = logging.INFO
    elif verbose == 2:
        loglevel = logging.DEBUG
    logger = logging.getLogger("")
    logger.setLevel(loglevel)
    # logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    # formatter = logging.Formatter(logformat, datefmt="%Y-%m-%d %H:%M:%S")
    # fh = logging.FileHandler("dacirco-scheduler.log", mode="w")
    # fh.setFormatter(formatter)
    # fh.setLevel(logging.DEBUG)
    # ch = logging.StreamHandler(stream=sys.stdout)
    # ch.setFormatter(formatter)
    # ch.setLevel(loglevel)
    # logger.addHandler(fh)
    # logger.addHandler(ch)
    g_grpc_server = grpc_server
    g_grpc_port = str(grpc_port)
    _logger.debug("grpc server: %s:%s", g_grpc_server, g_grpc_port)
    uvicorn.run(app, port=port, log_level="info")  # type: ignore
