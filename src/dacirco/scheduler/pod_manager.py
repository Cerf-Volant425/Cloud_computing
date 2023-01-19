import base64
import logging
from unicodedata import name
import uuid
import os
from typing import Optional
from datetime import timedelta

from kubernetes import client, config
from dacirco.scheduler.event_logger import EventLogger
from dacirco.scheduler.tc_worker_handler import TCWorkerHandler, TCWorkerStatus
from dacirco.scheduler.runner_manager import RunnerManager, RunnerManagerRes
from dacirco.scheduler.dacirco_dataclasses import MinIoParameters, PodParameters
from dacirco.scheduler.request_tracker import RequestTracker
from dacirco.scheduler.worker_tracker import WorkerTracker, WorkerStatus


_logger = logging.getLogger(__name__)


class PodManager(RunnerManager):
    def __init__(
        self,
        event_logger: EventLogger,
        request_tracker: RequestTracker,
        worker_tracker: WorkerTracker,
        pod_parameters: PodParameters,
        minio_parameters: MinIoParameters,
        max_workers: int,
        local_ip_address: str,
    ) -> None:
        self._name_counter = 0
        self._pod_count = 0
        self._request_tracker = request_tracker
        self._worker_tracker = worker_tracker
        self._MAX_WORKERS = max_workers
        self._NS_NAME = "dacirco"
        self._tc_workers: dict["str", "TCWorkerHandler"] = {}

        config.load_kube_config()  # type: ignore
        self._k8s_client = client.CoreV1Api()  # type: ignore
        namespaces = self._k8s_client.list_namespace()
        if not any(x.metadata.name == "dacirco" for x in namespaces.items):
            self._k8s_client.create_namespace(
                client.V1Namespace(metadata=client.V1ObjectMeta(name=self._NS_NAME))  # type: ignore
            )
        else:
            res = self._k8s_client.list_namespaced_pod(namespace=self._NS_NAME)
            for i in res.items:
                self._k8s_client.delete_namespaced_pod(i.metadata.name, self._NS_NAME)

        self._local_ip = local_ip_address
        self._event_logger = event_logger
        self._minio_parameters = minio_parameters
        self._pod_parameters = pod_parameters

    def create_runner(self) -> RunnerManagerRes:
        if self._pod_count == self._MAX_WORKERS:
            res = RunnerManagerRes(success=False, max_count_reached=True)
        else:
            self._pod_count += 1
            self._name_counter += 1
            pod_id = str(uuid.uuid4())
            pod_name = "pod-" + pod_id
            _logger.info("Starting new pod. Name: %s, id: %s", pod_name, str(pod_id))
            pod_env = [
                {"name": "TCWORKER_GRPC_SERVER", "value": self._local_ip},
                {"name": "TCWORKER_NAME", "value": pod_name},
                {"name": "TCWORKER_ID", "value": pod_id},
                {
                    "name": "TCWORKER_MINIO_SERVER",
                    "value": self._minio_parameters.server,
                },
                {
                    "name": "TCWORKER_MINIO_PORT",
                    "value": str(self._minio_parameters.port),
                },
                {
                    "name": "TCWORKER_MINIO_ACCESS_KEY",
                    "value": self._minio_parameters.access_key,
                },
                {
                    "name": "TCWORKER_MINIO_SECRET_KEY",
                    "value": self._minio_parameters.access_secret,
                },
            ]

            pod_affinity = {}
            if self._pod_parameters.spread_workers:
                pod_affinity = {
                    "podAntiAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 100,
                                "podAffinityTerm": {
                                    "labelSelector": {
                                        "matchExpressions": [
                                            {
                                                "key": "app",
                                                "operator": "In",
                                                "values": ["tc-worker"],
                                            },
                                        ]
                                    },
                                    "topologyKey": "kubernetes.io/hostname",
                                },
                            }
                        ]
                    }
                }
            if self._pod_parameters.concentrate_workers:
                pod_affinity = {
                    "podAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 100,
                                "podAffinityTerm": {
                                    "labelSelector": {
                                        "matchExpressions": [
                                            {
                                                "key": "app",
                                                "operator": "In",
                                                "values": ["tc-worker"],
                                            },
                                        ]
                                    },
                                    "topologyKey": "kubernetes.io/hostname",
                                },
                            }
                        ]
                    }
                }
            node_selector = {}
            if self._pod_parameters.node_selector:
                node_selector = {
                    "kubernetes.io/hostname": self._pod_parameters.node_selector
                }
            limits = {}
            limits["cpu"] = self._pod_parameters.cpu_limit
            limits["memory"] = self._pod_parameters.memory_limit
            requests = {}
            requests["cpu"] = self._pod_parameters.cpu_request
            requests["memory"] = self._pod_parameters.memory_request
            pod_manifest = {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": pod_name, "labels": {"app": "tc-worker"}},
                "spec": {
                    "affinity": pod_affinity,
                    "nodeSelector": node_selector,
                    "containers": [
                        {
                            "image": self._pod_parameters.image,
                            "name": pod_name,
                            "env": pod_env,
                            "resources": {
                                "limits": limits,
                                "requests": requests,
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                },
            }
            pod = self._k8s_client.create_namespaced_pod(
                body=pod_manifest, namespace=self._NS_NAME
            )

            logging.info("Pod created")
            worker = TCWorkerHandler(
                pod_name,
                pod_id,
                self._event_logger,
                self._request_tracker,
                self._worker_tracker,
            )
            self._tc_workers[pod_id] = worker
            res = RunnerManagerRes(
                success=True,
                max_count_reached=False,
                worker=worker,
            )
            self._event_logger.runner_created(name=pod_name, id=pod_id)
            node, cpus, memory = self.get_runner_info(pod_id)
            self._worker_tracker.add_worker(
                name=pod_name, id=pod_id, cpus=str(cpus), memory=memory, node=node
            )
        return res

    def get_tc_worker(self, worker_id: str) -> Optional[TCWorkerHandler]:
        res = None
        if worker_id not in self._tc_workers.keys():
            _logger.error("get_tc_worker: unknown worker: %s", worker_id)
        else:
            res = self._tc_workers[worker_id]
        return res

    def get_idle_worker(self) -> Optional[TCWorkerHandler]:
        """Returns the first available worker, None if all existing workers are free"""
        res = None
        for w in self._tc_workers.values():
            if w.status == TCWorkerStatus.IDLE:
                _logger.debug("Found available worker (name: %s, id: %s", w.name, w.id)
                res = w
        if res is None and self._pod_count <= self._MAX_WORKERS:
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
            pod_name = worker.name
            self._k8s_client.delete_namespaced_pod(pod_name, self._NS_NAME)
            _logger.info("Destroying worker pod: %s (%s)", worker.name, worker.id)

            self._event_logger.runner_destroyed(worker.name, worker_id)
            self._worker_tracker.set_worker_status(
                worker_id=worker_id, status=WorkerStatus.STOPPED
            )
            del self._tc_workers[worker_id]
            self._pod_count -= 1
        return res

    def destroy_idle_workers(self, threshold: timedelta):
        targets: list[str] = []
        for worker in self._tc_workers.values():
            if worker.get_idle_time() > threshold:
                targets.append(worker.id)
        for id in targets:
            self.destroy_runner(id)

    def get_runner_info(self, worker_id: str) -> tuple[str, str, str]:
        worker = self.get_tc_worker(worker_id=worker_id)
        node = ""
        cpu = "0"
        memory = "0"
        if worker:
            ret = self._k8s_client.read_namespaced_pod(
                name=worker.name, namespace=self._NS_NAME
            )
            node = ret.spec.node_name
            if "cpu" in ret.spec.containers[0].resources.requests.keys():
                cpu = ret.spec.containers[0].resources.requests["cpu"]
            if "memory" in ret.spec.containers[0].resources.requests.keys():
                memory = ret.spec.containers[0].resources.requests["memory"]
            if "cpu" in ret.spec.containers[0].resources.limits.keys():
                cpu += "_" + ret.spec.containers[0].resources.limits["cpu"]
            if "memory" in ret.spec.containers[0].resources.limits.keys():
                memory += "_" + ret.spec.containers[0].resources.limits["memory"]

        return (node, cpu, memory)
