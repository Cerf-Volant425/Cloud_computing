import click
import sys
import logging

from google.protobuf.symbol_database import Default
from dacirco.grpc_service.server import run_server
from dacirco.scheduler.dacirco_dataclasses import (
    VMParameters,
    MinIoParameters,
    RunnerType,
    SchedulerParameters,
    AlgorithmType,
    PodParameters,
)


def log_config(verbose: int) -> None:
    """Setup basic logging

    Args:
      verbose (int): the number of v's on the command line
    """
    loglevel = logging.WARN
    if verbose == 1:
        loglevel = logging.INFO
    elif verbose == 2:
        loglevel = logging.DEBUG
    logger = logging.getLogger("")
    logger.setLevel(logging.DEBUG)
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    formatter = logging.Formatter(logformat, datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler("dacirco-scheduler.log", mode="w")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setFormatter(formatter)
    ch.setLevel(loglevel)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("Started logging")


@click.command()
@click.option("-v", "--verbose", count=True, help="Increase verbosity")
@click.option(
    "-p",
    "--port",
    type=int,
    default=50051,
    show_default=True,
    help="The port number to bind to",
)
@click.option(
    "-r",
    "--runner-type",
    type=click.Choice(["vm", "pod"], case_sensitive=False),
    default="vm",
    help="The type of runner",
    show_default=True,
)
@click.option(
    "--os-image",
    default="dacirco",
    help="The image to use",
    show_default=True,
)
@click.option(
    "--os-flavor",
    default="m1.small",
    help="The flavor for the OpenStack VMs",
    show_default=True,
)
@click.option(
    "--os-key",
    default="mykey",
    help="The name of the OpenStack key to inject into the VM",
    show_default=True,
)
@click.option(
    "--os-security-group",
    default="ssh",
    help="The name of the OpenStack security group for the VM",
    show_default=True,
)
@click.option(
    "--os-network",
    default="external",
    help="The name of the OpenStack metwork to use",
    show_default=True,
)
@click.option(
    "--minio-server",
    help="The name (or IP address) of the MinIO server",
)
@click.option(
    "--minio-port",
    type=int,
    default=9000,
    show_default=True,
    help="The port number of the MinIO server",
)
@click.option(
    "--minio-access-key",
    default="minio",
    help="The access key for the MinIO server",
    show_default=True,
)
@click.option(
    "--minio-secret-key",
    default="pass4minio",
    help="The access secret for the MinIO server",
    show_default=True,
)
@click.option(
    "--max-workers",
    type=int,
    default=2,
    show_default=True,
    help="The maximum number of workers at any given time",
)
@click.option(
    "--ip-address",
    type=str,
    help="DO NOT USE THIS OPTION ON THE VDI VM (the IP address used by the workers)",
)
@click.option(
    "--call-interval",
    type=int,
    default=1,
    show_default=True,
    help="How many seconds between calls of the scheduler periodic_function",
)
@click.option(
    "--max-idle-time",
    type=int,
    default=60,
    show_default=True,
    help="How many seconds before a worker is destroyed (grace period algorithm)",
)
@click.option(
    "--event-log-file",
    help="The name of the event log file (optional)",
)
@click.option(
    "--algorithm",
    type=click.Choice(["minimize-workers", "grace-period"], case_sensitive=False),
    default="minimize-workers",
    help="The scheduling algorithm",
    show_default=True,
)
@click.option(
    "--pod-image",
    default="gitlab-devops.cloud.rennes.enst-bretagne.fr:4567/devops/shared/dacirco:latest",
    help="The image to use",
    show_default=True,
)
@click.option(
    "--pod-cpu-request",
    default="1",
    show_default=True,
    help="The minimum number of vCPUs of each pod",
)
@click.option(
    "--pod-memory-request",
    default="1.4G",
    show_default=True,
    help="The memory request for each pod",
)
@click.option(
    "--pod-cpu-limit",
    default="4",
    show_default=True,
    help="The maximum number of vCPUs of each pod",
)
@click.option(
    "--pod-memory-limit",
    default="4G",
    show_default=True,
    help="The maximum memory for each pod",
)
@click.option(
    "--pod-node-selector",
    help="[EXPERT] The node name where pods must run",
)
@click.option(
    "--concentrate-workers",
    is_flag=True,
    help="Try to place all the workers on the same node",
)
@click.option(
    "--spread-workers",
    is_flag=True,
    help="Try to place workers on different nodes",
)
def run(
    verbose: int,
    port: int,
    runner_type: str,
    os_image: str,
    os_flavor: str,
    os_key: str,
    os_security_group: str,
    os_network: str,
    minio_server: str,
    minio_port: int,
    minio_access_key: str,
    minio_secret_key: str,
    max_workers: int,
    ip_address: str,
    call_interval: int,
    event_log_file: str,
    algorithm: str,
    max_idle_time: int,
    pod_image: str,
    pod_cpu_request: str,
    pod_memory_request: str,
    pod_cpu_limit: str,
    pod_memory_limit: str,
    pod_node_selector: str,
    concentrate_workers: bool,
    spread_workers: bool,
) -> None:
    if not minio_server:
        print(
            "You must use the --minio-server option to specify the IP address of the MinIO server"
        )
        raise SystemExit
    if concentrate_workers and spread_workers:
        print(
            "You cannot set both concentrate_workers and spread_workers, pick only one!"
        )
        raise SystemExit
    log_config(verbose)
    vm_parameters = VMParameters(
        image=os_image,
        flavor=os_flavor,
        key=os_key,
        security_group=os_security_group,
        network=os_network,
        concentrate_workers=concentrate_workers,
        spread_workers=spread_workers,
    )
    minio_parameters = MinIoParameters(
        server=minio_server,
        port=minio_port,
        access_key=minio_access_key,
        access_secret=minio_secret_key,
    )
    pod_parameters = PodParameters(
        image=pod_image,
        cpu_limit=pod_cpu_limit,
        memory_limit=pod_memory_limit,
        cpu_request=pod_cpu_request,
        memory_request=pod_memory_request,
        node_selector=pod_node_selector,
        concentrate_workers=concentrate_workers,
        spread_workers=spread_workers,
    )
    rnr_type = RunnerType.VM
    if runner_type.lower() == "pod":
        rnr_type = RunnerType.POD
    algorithm_type = AlgorithmType.MINIMIZE_WORKERS
    if algorithm.lower() == "grace-period":
        algorithm_type = AlgorithmType.GRACE_PERIOD
    sched_parameters = SchedulerParameters(
        algorithm=algorithm_type,
        max_workers=max_workers,
        call_interval=call_interval,
        max_idle_time=max_idle_time,
        runner_type=rnr_type,
    )

    run_server(
        vm_parameters=vm_parameters,
        minio_parameters=minio_parameters,
        port=port,
        ip_address=ip_address,
        event_log_file=event_log_file,
        sched_parameters=sched_parameters,
        pod_parameters=pod_parameters,
    )
