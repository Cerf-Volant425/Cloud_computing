#!/usr/bin/env python3

from lib2to3.pytree import Base
from fastapi import Path, Body
from pydantic import BaseModel, Field
from typing import List


class Job(BaseModel):
    """Defines the type parameter of /jobs requests."""

    id_video: str = Field(None, title="Resource Path of the Video", max_length=256)
    """ resource Path of the Video
    """

    bitrate: int = Field(
        None,
        title="Bitrate of the Requested Video",
        gt=0,
        description="The bitrate must be in [500, 8000]",
    )
    """ bitrate of the Requested Video. Must be in range [500, 8000]
    """
    speed: str = Field(
        None, title="Encoding Speed", description="It can be ultrafast or fast"
    )
    """ encoding speed. Can be "ultrafast" or "fast"
    """


class RequestIDsListRest(BaseModel):
    items: list[str]
    has_more: bool


class RequestState:
    """Possible states for a Job."""

    WAITING = "Waiting"
    STARTED = "Started"
    COMPLETED = "Completed"
    ERROR = "Error"


class Worker(BaseModel):
    """Defines the type parameter of /workers requests"""

    name: str = Field(None, title="The worker name", max_length=256)
    """The worker name"""

    id: str = Field(None, title="The wroker ID", max_length=256)
    """The worker ID"""

    cpus: str = Field(None, title="The number of vcpus", max_length=20)
    """The number of vcpus of the worker"""

    memory: str = Field(None, title="The memory of the worker", max_length=100)
    """The memory of the worker"""

    node: str = Field(None, title="The node running the worker", max_length=200)
    """The node running the worker"""


class WorkerIDsListRest(BaseModel):
    items: list[str]
    has_more: bool


class WorkerState:
    """Possible states for a worker."""

    BOOTING = "Booting"
    DOWNLOADING_FILE = "Downloading File"
    TRANSCODING = "Transcoding"
    UPLOADING_FILE = "Uploading File"
    IDLE = "Idle"
    STOPPED = "Stopped"
