#!/usr/bin/env python3

import click
import httpx
import asyncio
from tabulate import tabulate
from dataclasses import dataclass


@dataclass
class Worker:
    name: str
    id: str
    cpus: str
    memory: str
    node: str
    state: str


@dataclass
class Job:
    id: str
    file_name: str
    bitrate: str
    speed: str
    state: str


class RestMonitor:
    def __init__(
        self, workers_base_url: str, jobs_base_url: str, refresh_interval: float
    ):
        self.workers_base_url = workers_base_url
        self.jobs_base_url = jobs_base_url
        self.refresh_interval = refresh_interval
        self.jobs_dict: dict[str, Job] = {}
        self.workers_dict: dict[str, Worker] = {}

    async def get_worker(self, worker_id: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.workers_base_url}/{worker_id}", timeout=15.0
            )
            response_dict = response.json()
            return response_dict

    async def get_worker_state(self, worker_id: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.workers_base_url}/{worker_id}/state", timeout=15.0
            )
            state = response.json()
            return {"id": worker_id, "state": state}

    async def get_workers(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(self.workers_base_url, timeout=15.0)
            response_dict = response.json()
            worker_id_list = response_dict["items"]

        workers_task_list = []
        for worker_id in worker_id_list:
            workers_task_list.append(self.get_worker(worker_id=worker_id))
        worker_list = await asyncio.gather(*workers_task_list)
        worker_state_task_list = []
        for worker_id in worker_id_list:
            worker_state_task_list.append(self.get_worker_state(worker_id=worker_id))
        worker_state_list = await asyncio.gather(*worker_state_task_list)
        workers_dict: dict[str, Worker] = {}

        for w in worker_list:
            worker = Worker(
                name=w["name"],
                id=w["id"],
                cpus=w["cpus"],
                memory=w["memory"],
                node=w["node"],
                state="",
            )
            workers_dict[w["id"]] = worker
        for w in worker_state_list:
            workers_dict[w["id"]].state = w["state"]
        self.workers_dict = workers_dict
        print(tabulate(self.workers_dict.values(), headers="keys"))  # type: ignore

    async def get_job(self, job_id: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.jobs_base_url}/{job_id}", timeout=15.0)
            response_dict = response.json()
            return {"id": job_id, "job_desc": response_dict}

    async def get_job_state(self, job_id: str):
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.jobs_base_url}/{job_id}/state", timeout=15.0
            )
            state = response.json()
            return {"id": job_id, "state": state}

    async def get_jobs(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(self.jobs_base_url, timeout=15.0)
            response_dict = response.json()
            job_id_list = response_dict["items"]
        jobs_task_list = []
        for job_id in job_id_list:
            jobs_task_list.append(self.get_job(job_id=job_id))
        job_list = await asyncio.gather(*jobs_task_list)
        job_state_task_list = []
        for job_id in job_id_list:
            job_state_task_list.append(self.get_job_state(job_id=job_id))
        job_state_list = await asyncio.gather(*job_state_task_list)
        jobs_dict: dict[str, Job] = {}

        for j in job_list:
            job = Job(
                id=j["id"],
                file_name=j["job_desc"]["id_video"],
                bitrate=j["job_desc"]["bitrate"],
                speed=j["job_desc"]["speed"],
                state="",
            )
            jobs_dict[j["id"]] = job
        for j in job_state_list:
            jobs_dict[j["id"]].state = j["state"]
        self.jobs_dict = jobs_dict
        print(tabulate(self.jobs_dict.values(), headers="keys"))  # type: ignore

    async def get_all(self):
        while True:
            await asyncio.gather(
                self.get_workers(),
                self.get_jobs(),
            )
            await asyncio.sleep(self.refresh_interval)


@click.command()
@click.option(
    "-p",
    "--port",
    type=int,
    default=8000,
    show_default=True,
    help="The port number of the REST server",
)
@click.option(
    "--rest-server",
    default="localhost",
    show_default=True,
    help="The name (or IP address) of the REST server",
)
@click.option(
    "--refresh-interval",
    default="3",
    show_default=True,
    type=float,
    help="How often to fetch the data (in seconds)",
)
def run_rest_monitor(port: int, rest_server: str, refresh_interval: float):
    workers_base_url: str = f"http://{rest_server}:{port}/workers"
    jobs_base_url: str = f"http://{rest_server}:{port}/jobs"
    rest_monitor = RestMonitor(
        workers_base_url=workers_base_url,
        jobs_base_url=jobs_base_url,
        refresh_interval=refresh_interval,
    )
    asyncio.run(rest_monitor.get_all())
