import click
import pandas as pd
from tabulate import tabulate
from dacirco.scheduler.event_logger import EventCodes


@click.command()
@click.argument("input_log_filename", type=click.Path(exists=True))
def process_event_log(input_log_filename: click.Path):
    print(f"Processing event log file: {input_log_filename}")
    df = pd.read_json(input_log_filename, lines=True)
    df["time"] = pd.to_datetime(df.time).dt.tz_localize("CET")
    # Remove unsuccessful requests
    if "error_message" in df.columns:
        err_task_ids = df[~df["error_message"].isnull()]["task_id"]
        err_req_ids = df[(df["task_id"].isin(err_task_ids)) & (df["code"] == "EV3")][
            "request_id"
        ]
        err_logs = df[
            (df["task_id"].isin(err_task_ids)) | (df["request_id"].isin(err_req_ids))
        ]
        print(f"Found {err_logs.shape[0]} failed requests (errors)")
        ok_logs = df.drop(err_logs.index)

    else:
        ok_logs = df

    # Get all the events concerning workers
    worker_df = ok_logs[
        ok_logs["code"].isin(
            [
                EventCodes.RUNNER_CREATED,
                EventCodes.RUNNER_REGISTERED,
                EventCodes.RUNNER_DESTROYED,
            ]
        )
    ].copy()
    worker_df["timediff"] = worker_df.groupby("worker_id").time.diff()
    worker_df.drop(
        columns=["request_id", "input_video", "bitrate", "speed", "task_id"],
        inplace=True,
    )
    worker_df = (
        worker_df.pivot(index="worker_id", columns="code")
        .loc[
            :,
            [
                ("time", EventCodes.RUNNER_REGISTERED),
                ("time", EventCodes.RUNNER_CREATED),
                ("time", EventCodes.RUNNER_DESTROYED),
                ("name", EventCodes.RUNNER_CREATED),
                ("node", EventCodes.RUNNER_REGISTERED),
                ("cpus", EventCodes.RUNNER_REGISTERED),
                ("memory", EventCodes.RUNNER_REGISTERED),
                ("timediff", EventCodes.RUNNER_REGISTERED),
                ("timediff", EventCodes.RUNNER_DESTROYED),
            ],
        ]
        .rename(
            columns={
                EventCodes.RUNNER_REGISTERED: "registered",
                EventCodes.RUNNER_CREATED: "created",
                EventCodes.RUNNER_DESTROYED: "destroyed",
            }
        )
    )
    worker_df.columns = ["_".join(col) for col in worker_df.columns.to_flat_index()]
    worker_df.rename(
        columns={
            "name_created": "name",
            "node_registered": "node",
            "memory_registered": "memory",
            "cpus_registered": "cpus",
        },
        inplace=True,
    )
    worker_df.reset_index(inplace=True)
    worker_df.sort_values(by="time_registered", inplace=True)
    worker_df.to_csv(input_log_filename[:-4] + "-workers.csv", index=False)

    ok_logs = ok_logs[
        ok_logs["code"].isin(
            [
                EventCodes.REQUEST_RECEIVED,
                EventCodes.TASK_ASSIGNED,
                EventCodes.TASK_STARTED,
                EventCodes.FILE_DOWNLOADED,
                EventCodes.TRANSCODING_COMPLETED,
                EventCodes.FILE_UPLOADED,
            ]
        )
    ].copy()

    ok_logs.drop(columns=["name", "node", "cpus", "memory"], inplace=True)
    ok_logs["timediff"] = ok_logs.groupby("request_id").time.diff()

    ok_logs = (
        ok_logs.pivot(index="request_id", columns="code")
        .loc[
            :,
            [
                ("time", EventCodes.REQUEST_RECEIVED),
                ("time", EventCodes.TASK_ASSIGNED),
                ("time", EventCodes.TASK_STARTED),
                ("time", EventCodes.FILE_DOWNLOADED),
                ("time", EventCodes.TRANSCODING_COMPLETED),
                ("time", EventCodes.FILE_UPLOADED),
                ("input_video", EventCodes.REQUEST_RECEIVED),
                ("bitrate", EventCodes.REQUEST_RECEIVED),
                ("speed", EventCodes.REQUEST_RECEIVED),
                ("worker_id", EventCodes.TASK_ASSIGNED),
                ("task_id", EventCodes.TASK_ASSIGNED),
                ("timediff", EventCodes.TASK_ASSIGNED),
                ("timediff", EventCodes.TASK_STARTED),
                ("timediff", EventCodes.FILE_DOWNLOADED),
                ("timediff", EventCodes.TRANSCODING_COMPLETED),
                ("timediff", EventCodes.FILE_UPLOADED),
            ],
        ]
        .rename(
            columns={
                EventCodes.REQUEST_RECEIVED: "received",
                EventCodes.TASK_ASSIGNED: "assigned",
                EventCodes.TASK_STARTED: "started",
                EventCodes.FILE_DOWNLOADED: "file_downloaded",
                EventCodes.TRANSCODING_COMPLETED: "transcoding_done",
                EventCodes.FILE_UPLOADED: "file_uploaded",
            }
        )
    )
    ok_logs.columns = ["_".join(col) for col in ok_logs.columns.to_flat_index()]
    ok_logs.rename(
        columns={
            "input_video_received": "input_video",
            "bitrate_received": "bitrate",
            "speed_received": "speed",
            "worker_id_assigned": "worker_id",
            "task_id_assigned": "task_id",
        },
        inplace=True,
    )
    ok_logs.sort_values(by="time_received", inplace=True)
    ok_logs.to_csv(input_log_filename[:-4] + "-ok-requests.csv", index=False)


if __name__ == "__main__":
    process_event_log()
