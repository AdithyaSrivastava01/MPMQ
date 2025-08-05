#!/usr/bin/env bash
set -euo pipefail
python -m grpc_tools.protoc \
  -I proto \
  --python_out=mpmq \
  --grpc_python_out=mpmq \
  proto/queue.proto
echo "[ok] generated mpmq/queue_pb2.py and mpmq/queue_pb2_grpc.py"
