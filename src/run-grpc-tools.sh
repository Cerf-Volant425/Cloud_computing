#!/usr/bin/sh

# For some reason we have to run grpc_tools from this directory (src) if we want
# the import in dacirco_pb2_grpc.py to be correct when using pip install -e .
# Found this here: https://github.com/grpc/grpc/issues/9575


# Install mypy-protobuf with pip for this to work (otherwise remove --mypy_out)
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. --mypy_out=. dacirco/proto/dacirco.proto

