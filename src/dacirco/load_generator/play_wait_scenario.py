import click
import csv
import time
import requests

from dacirco.rest_api.rest_api_schema import RequestState


@click.command("cli", context_settings={"show_default": True})
@click.argument(
    "file_name",
    type=click.Path(exists=True),
    # help="The scenario file name"
)
@click.option(
    "--api-server",
    default="localhost",
    help="The name (or IP address of the DaCirco API server",
)
@click.option(
    "--api-port",
    default=8000,
    type=int,
    help="The port number of the DaCirco API server",
)
def play_wait_scenario(file_name: str, api_server: str, api_port):
    click.echo(f"Playing the waiting scenario in {file_name}")
    print(RequestState.COMPLETED)
    with open(file_name, "r") as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        for row in reader:
            if len(row) != 4 or row[0][0] == "#":
                continue
            movie, bitrate, preset = row[1:]
            bitrate = int(bitrate)
            data = {
                "id_video": movie,
                "bitrate": bitrate,
                "speed": preset,
            }

            click.echo(f"Sending new request: {data}")

            url = f"http://{api_server}:{api_port}/jobs"
            r = requests.post(url, json=data)
            if r.status_code != requests.codes.created:
                click.echo(f"Request failed ({r.status_code =}).  Terminating")
                raise SystemExit
            location = r.headers["Location"]
            request_done = False
            while not request_done:
                time.sleep(1)
                request_url = f"{location}/state"
                r = requests.get(request_url)
                if (
                    r.text.strip('"') == RequestState.COMPLETED
                    or r.text.strip('"') == RequestState.ERROR
                ):
                    click.echo(f"Request done, status: {r.text}")
                    request_done = True


if __name__ == "__main__":
    play_wait_scenario()
