import click
import grpc


from dacirco.proto.dacirco_pb2 import TCRequest
from dacirco.proto.dacirco_pb2_grpc import DaCircogRPCServiceStub


@click.command("cli", context_settings={"show_default": True})
@click.option("--input-video", default="test-1-10s.mov", help="The video id")
@click.option("--output-video", default="out-test-1-10s.mov", help="The video id")
@click.option("--rate", default=7000, help="The desired bitrate")
@click.option("--speed", default="fast", help="The desired speed")
def submit_request(input_video: str, output_video: str, rate: int, speed: str):
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = DaCircogRPCServiceStub(channel)
        response = stub.submit_request(
            TCRequest(
                input_video=input_video,
                bitrate=rate,
                speed=speed,
                output_video=output_video,
            )
        )
    print(f"TC client received: {response.success}")


if __name__ == "__main__":
    submit_request()
