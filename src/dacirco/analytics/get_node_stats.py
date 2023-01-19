import click
import httpx
import math
import sys
import pandas as pd
import numpy as np
from functools import partial
from urllib.parse import unquote


# For historical reasons, we need to change the node names with k8s
def change_compute_name(name: str) -> str:
    res = name
    if "k8s-worker" in name:
        res = name.replace("k8s-worker", "compute")
    return res


def p_get_gauge_fun(
    p_server: str, fun: str, gauge: str, node: str, end: str, duration: str
):
    if not node:
        return (np.nan, np.nan, np.nan)
    node = change_compute_name(node)
    params = {
        "query": f'{fun}({gauge}{{instance="{node}:9100"}}[{duration}s])',
        "time": end,
    }
    res = httpx.get(f"http://{p_server}:9090/api/v1/query", params=params)
    res_json = res.json()
    if res.status_code != 200 or not res_json["data"]["result"]:
        print("Error while fetching data")
        print("Request url: ", unquote(str(res.url)))
        print("Response: ", res_json)
        sys.exit()
    else:
        return round(float(res_json["data"]["result"][0]["value"][1]), 2)


def p_get_memory_fun(p_server: str, fun: str, node: str, end: str, duration: str):
    if not node:
        return (np.nan, np.nan, np.nan)
    node = change_compute_name(node)
    mem_query = (
        f'node_memory_MemTotal_bytes{{instance="{node}:9100"}}'
        f'-node_memory_MemFree_bytes{{instance="{node}:9100"}}'
        f'-node_memory_Buffers_bytes{{instance="{node}:9100"}}'
        f'-node_memory_Cached_bytes{{instance="{node}:9100"}}'
    )

    params = {
        "query": f"{fun}(({mem_query})[{duration}s:10s])",
        "time": end,
    }
    res = httpx.get(f"http://{p_server}:9090/api/v1/query", params=params)
    res_json = res.json()
    if res.status_code != 200 or not res_json["data"]["result"]:
        print("Error while fetching data")
        print("Request url: ", unquote(str(res.url)))
        print("Response: ", res_json)
        sys.exit()
    else:
        return round(float(res_json["data"]["result"][0]["value"][1]) / (10 ** 9), 2)


def get_load1(row: pd.DataFrame, p_server: str):
    node = row["node"]  # type: ignore
    t_td = row["time_transcoding_done"].isoformat()  # type: ignore
    duration = str(math.ceil(row["timediff_transcoding_done"].total_seconds()))  # type: ignore

    res = tuple(
        p_get_gauge_fun(
            p_server=p_server,
            fun=f,
            gauge="node_load1",
            node=node,  # type: ignore
            end=t_td,
            duration=duration,
        )
        for f in ["min_over_time", "avg_over_time", "max_over_time"]
    )

    return res


def get_memory(row: pd.DataFrame, p_server: str):
    node = row["node"]  # type: ignore
    t_td = row["time_transcoding_done"].isoformat()  # type: ignore
    duration = str(math.ceil(row["timediff_transcoding_done"].total_seconds()))  # type: ignore

    res = tuple(
        p_get_memory_fun(
            p_server=p_server,
            fun=f,
            node=node,  # type: ignore
            end=t_td,
            duration=duration,
        )
        for f in ["min_over_time", "avg_over_time", "max_over_time"]
    )

    return res


@click.command()
@click.argument("input_csv_filename", type=click.Path(exists=True))
@click.option(
    "-s",
    "--prometheus-server",
    default="controller",
    help="The hostname or IP address of the Prometheus server",
)
@click.option(
    "--disable-memory-stats",
    is_flag=True,
    help="The hostname or IP address of the Prometheus server",
)
def process_csv_file(
    input_csv_filename: click.Path, prometheus_server: str, disable_memory_stats: bool
):
    ok_logs_csv_path = input_csv_filename
    workers_csv_path = str(input_csv_filename).replace("ok-requests.csv", "workers.csv")
    print(f"Processing csv files: {input_csv_filename}, {workers_csv_path}")
    date_cols = [
        "time_received",
        "time_assigned",
        "time_started",
        "time_file_downloaded",
        "time_transcoding_done",
        "time_file_uploaded",
    ]
    timedelta_cols = [
        "timediff_assigned",
        "timediff_started",
        "timediff_file_downloaded",
        "timediff_transcoding_done",
        "timediff_file_uploaded",
    ]
    converters = {name: pd.to_timedelta for name in timedelta_cols}
    req = pd.read_csv(
        input_csv_filename,  # type: ignore
        parse_dates=date_cols,  # type: ignore
        converters=converters,  # type: ignore
    )
    w_date_cols = ["time_registered", "time_created", "time_destroyed"]
    w_timedelta_cols = ["timediff_registered", "timediff_destroyed"]
    w_converters = {name: pd.to_timedelta for name in w_timedelta_cols}
    workers = pd.read_csv(
        workers_csv_path,  # type: ignore
        parse_dates=w_date_cols,  # type: ignore
        converters=w_converters,  # type: ignore
    )

    # Add the name of the worker and the node hosting it by joining the req
    # dataframe with the worker dataframe.
    req_w = pd.merge(
        req, workers[["worker_id", "name", "node"]], on="worker_id", how="left"
    )

    # Add three columns with the node_load1 min, avg, max
    req_w[["load1_min", "load1_avg", "load1_max"]] = req_w.apply(
        partial(get_load1, p_server=prometheus_server),  # type: ignore
        axis=1,
        result_type="expand",  # type: ignore
    )

    if not disable_memory_stats:
        # Add three columns with the used memory min, avg, max
        req_w[["used_mem_min", "used_mem_avg", "used_mem_max"]] = req_w.apply(
            partial(get_memory, p_server=prometheus_server),  # type: ignore
            axis=1,
            result_type="expand",  # type: ignore
        )

    req_w.to_csv(
        str(input_csv_filename).replace(
            "ok-requests.csv", "ok-requests-node-stats.csv"
        ),
        index=False,
    )


if __name__ == "__main__":
    process_csv_file()
