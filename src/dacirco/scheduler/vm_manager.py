import base64
import logging
import uuid
import os
from typing import Optional
from datetime import datetime, timedelta

import openstack  # type: ignore
from dacirco.scheduler.event_logger import EventLogger
from dacirco.scheduler.tc_worker_handler import TCWorkerHandler, TCWorkerStatus
from dacirco.scheduler.runner_manager import RunnerManager, RunnerManagerRes
from dacirco.scheduler.dacirco_dataclasses import MinIoParameters, VMParameters
from dacirco.scheduler.request_tracker import RequestTracker, RequestStatus
from dacirco.scheduler.worker_tracker import WorkerTracker, WorkerStatus


_logger = logging.getLogger(__name__)


class VMManager(RunnerManager):
    def __init__(
        self,
        event_logger: EventLogger,
        request_tracker: RequestTracker,
        worker_tracker: WorkerTracker,
        vm_parameters: VMParameters,
        minio_parameters: MinIoParameters,
        max_workers: int,
        local_ip_address: str,
    ) -> None:
        self._name_counter = 0
        self._vm_count = 0
        self._request_tracker = request_tracker
        self._worker_tracker = worker_tracker
        self._MAX_WORKERS = max_workers
        self._admin_os_client = None
        try:
            self._os_client = openstack.connect(cloud="openstack")
        except Exception as exc:
            _logger.error(
                "Something went wrong when setting up the OpenStack connection: %s", exc
            )
            raise SystemExit
        try:
            self._admin_os_client = openstack.connect(cloud="openstack_admin")
            _logger.info("OpenStack admin access enabled")
        except Exception as exc:
            _logger.error(
                "Something went wrong when setting up the OpenStack admin connection: %s",
                exc,
            )

        self._tc_workers: dict["str", "TCWorkerHandler"] = {}
        self._vms: dict["str", "openstack.compute.v2.server.Server"] = {}  # type:ignore
        # Initialize the OpenStack client
        try:
            self._os_image = self._os_client.compute.find_image(  # type:ignore
                vm_parameters.image
            )  # type:ignore
            if not self._os_image:
                self._resource_not_found("Image", vm_parameters.image)
            self._os_flavor = self._os_client.compute.find_flavor(  # type:ignore
                vm_parameters.flavor
            )  # type:ignore
            if not self._os_flavor:
                self._resource_not_found("Flavor", vm_parameters.flavor)
            self._os_network = self._os_client.network.find_network(  # type:ignore
                vm_parameters.network
            )
            if not self._os_network:
                self._resource_not_found("Network", vm_parameters.network)
            self._os_keypair = self._os_client.compute.find_keypair(  # type:ignore
                vm_parameters.key
            )  # type:ignore
            if not self._os_keypair:
                self._resource_not_found("Keypair", vm_parameters.key)
            self._os_secgroup = (
                self._os_client.network.find_security_group(  # type:ignore
                    vm_parameters.security_group
                )
            )
            if not self._os_secgroup:
                self._resource_not_found("Security group", vm_parameters.security_group)
        except Exception as exc:
            _logger.error(
                "Something went wrong when getting the OpenStack resources: %s", exc
            )
            raise SystemExit
        self._local_ip = local_ip_address
        self._event_logger = event_logger
        self._minio_parameters = minio_parameters

    def _resource_not_found(self, name: str, value: str):
        _logger.error(f"OpenStack: {name} (value: {value}) not found")
        raise SystemExit

    def create_runner(self) -> RunnerManagerRes:
        if self._vm_count == self._MAX_WORKERS:
            res = RunnerManagerRes(success=False, max_count_reached=True)
        else:
            self._vm_count += 1
            self._name_counter += 1
            vm_name = "vm-" + str(self._name_counter)
            vm_id = str(uuid.uuid4())
            _logger.info("Starting new VM. Name: %s, id: %s", vm_name, str(vm_id))
            commands = f"#!/bin/bash\ntee /etc/dacirco/environment <<EOF\n"
            commands += f"TCWORKER_GRPC_SERVER={self._local_ip}\n"
            commands += f"TCWORKER_NAME={vm_name}\n"
            commands += f"TCWORKER_ID={vm_id}\n"
            commands += f"TCWORKER_MINIO_SERVER={self._minio_parameters.server}\n"
            commands += f"TCWORKER_MINIO_PORT={self._minio_parameters.port}\n"
            commands += (
                f"TCWORKER_MINIO_ACCESS_KEY={self._minio_parameters.access_key}\n"
            )
            commands += (
                f"TCWORKER_MINIO_SECRET_KEY={self._minio_parameters.access_secret}\n"
            )
            commands += f"TCWORKER_LOG_FILE=/home/ubuntu/dacirco_tc_worker.log\n"
            commands += f"EOF"

            userdata = userdata = base64.b64encode(commands.encode("utf-8")).decode(
                "utf-8"
            )
            server: openstack.compute.v2.server.Server = self._os_client.compute.create_server(  # type: ignore  # type:ignore
                name=vm_name,
                image_id=self._os_image.id,
                flavor_id=self._os_flavor.id,
                networks=[{"uuid": self._os_network.id}],
                key_name=self._os_keypair.name,
                user_data=userdata,
                security_groups=[{"name": self._os_secgroup.name}],
            )
            node = ""
            if self._admin_os_client:
                # For some unknown reason, the hypervisor_hostname is often None
                # hence we try multiple times (usually it works after 4 or 5 times)
                server = self._admin_os_client.compute.get_server(  # type:ignore
                    server.id
                )  # type:ignore
                node = server.hypervisor_hostname
                max_count = 30
                count = 1
                while node is None and count <= max_count:
                    server = self._admin_os_client.compute.get_server(  # type:ignore
                        server.id
                    )  # type:ignore
                    node = server.hypervisor_hostname
                    count += 1
                if node is None:
                    _logger.warning(
                        "Failed to get node name, even if admin access enabled"
                    )
            else:
                _logger.warning("No OpenStack admin access, cannot get node running VM")

            worker = TCWorkerHandler(
                vm_name,
                vm_id,
                self._event_logger,
                self._request_tracker,
                self._worker_tracker,
            )
            self._tc_workers[vm_id] = worker
            self._vms[vm_id] = server
            res = RunnerManagerRes(
                success=True,
                max_count_reached=False,
                worker=worker,
            )
            self._event_logger.runner_created(name=vm_name, id=vm_id)
            self._worker_tracker.add_worker(
                name=vm_name,
                id=vm_id,
                memory=server.flavor["ram"],
                cpus=server.flavor["vcpus"],
                node=node,
            )
        return res

    def get_tc_worker(self, worker_id: str) -> Optional[TCWorkerHandler]:
        res = None
        if worker_id not in self._tc_workers.keys():
            _logger.error("get_tc_worker: unknown worker: %s", worker_id)
        else:
            res = self._tc_workers[worker_id]
        return res

    def get_vm(self, vm_id: str) -> openstack.compute.v2.server.Server:  # type:ignore
        res = None
        if vm_id not in self._tc_workers.keys():
            _logger.error("get_tc_worker: unknown worker: %s", vm_id)
        else:
            res = self._vms[vm_id]
        return res

    def get_idle_worker(self) -> Optional[TCWorkerHandler]:
        """Returns the first available worker, None if all existing workers are busy"""
        res = None
        for w in self._tc_workers.values():
            if w.status == TCWorkerStatus.IDLE:
                _logger.debug("Found available worker (name: %s, id: %s", w.name, w.id)
                res = w
        if res is None and self._vm_count <= self._MAX_WORKERS:
            self.create_runner()
        return res

    def destroy_runner(self, worker_id: str) -> RunnerManagerRes:
        res = RunnerManagerRes(success=True)
        if worker_id not in self._tc_workers.keys():
            msg = f"Destroy VM, asked to destroy unknown worker. Id: {worker_id}"
            _logger.error(msg)
            res.success = False
            res.error_message = msg
        else:
            worker = self._tc_workers[worker_id]
            vm = self._vms[worker_id]
            _logger.info("Destroying worker VM: %s (%s)", worker.name, worker.id)
            self._os_client.compute.delete_server(vm)  # type:ignore
            self._event_logger.runner_destroyed(worker.name, worker_id)
            self._worker_tracker.set_worker_status(
                worker_id=worker_id, status=WorkerStatus.STOPPED
            )
            del self._tc_workers[worker_id]
            del self._vms[worker_id]
            self._vm_count -= 1
        return res

    def destroy_idle_workers(self, threshold: timedelta):
        targets: list[str] = []
        for worker in self._tc_workers.values():
            if worker.get_idle_time() > threshold:
                targets.append(worker.id)
        for id in targets:
            self.destroy_runner(id)

    def get_runner_info(self, worker_id: str) -> tuple[str, str, str]:
        server = self.get_vm(vm_id=worker_id)
        try:
            node = server.hypervisor_hostname
        except AttributeError:
            node = ""
        cpus = str(server.flavor["vcpus"])
        memory = str(server.flavor["ram"])
        return (node, cpus, memory)
