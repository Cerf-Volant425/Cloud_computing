import base64
import logging
import subprocess
import sys
import uuid
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, TypeVar

import docker
import openstack  # type: ignore
from dacirco.scheduler.event_logger import EventLogger
from dacirco.scheduler.tc_worker_handler import TCWorkerHandler, TCWorkerStatus
from dacirco.scheduler.dacirco_dataclasses import (
    MinIoParameters,
    VMParameters,
    RunnerType,
)

_logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RunnerManagerRes:
    success: bool
    max_count_reached: bool = False
    worker: Optional[TCWorkerHandler] = None
    error_message: Optional[str] = None


class RunnerManager(ABC):
    @abstractmethod
    def create_runner(self) -> RunnerManagerRes:
        pass

    @abstractmethod
    def get_tc_worker(self, worker_id: str) -> Optional[TCWorkerHandler]:
        pass

    @abstractmethod
    def get_idle_worker(self) -> Optional[TCWorkerHandler]:
        pass

    @abstractmethod
    def destroy_runner(self, worker_id: str) -> RunnerManagerRes:
        pass

    @abstractmethod
    def get_runner_info(self, worker_id: str) -> tuple[str, str, str]:
        pass
