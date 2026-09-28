#!/usr/bin/env bash
# Builds terraform/.build/pyiceberg_layer/python/: pyiceberg, pyarrow and their dependencies as Linux x86_64 wheels for
# Python 3.11, the consume_sensor_readings Lambda's runtime. Run it before `terraform plan` when the layer is new or its
# requirements change; Terraform zips the folder and publishes a layer version when the content changes.
#
# boto3 is left out: the Lambda runtime has it. A Lambda allows 250 MB unzipped with all its layers; this is ~231 MB,
# pyarrow alone 140 MB (see src/00_bronze/_streaming/requirements.txt for why pyarrow 20).
set -euo pipefail
cd "$(dirname "$0")/.."
target=terraform/.build/pyiceberg_layer/python
rm -rf terraform/.build/pyiceberg_layer
python3.11 -m pip install --quiet --requirement src/00_bronze/_streaming/requirements.txt --target "$target" \
  --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all:
cd "$target"
rm -rf boto3* botocore* s3transfer* bin
find . -name "__pycache__" -type d -prune -exec rm -rf {} +
echo "pyiceberg layer: $(du -sh . | cut -f1) in $target"
