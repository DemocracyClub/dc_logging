#!/bin/bash
set -euxo pipefail

# Moto needs a special image for running lamda's as part of a test suite
# These are third party supported. The default ones don't support python 3.12.
# This one does, but might need revisiting.
# Context: https://github.com/getmoto/moto/issues/8236
MOTO_DOCKER_LAMBDA_IMAGE=shogo82148/lambda-python:3.12.2024.10.18 uv run pytest