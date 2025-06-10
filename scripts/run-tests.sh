#!/bin/bash
set -euxo pipefail

MOTO_DOCKER_LAMBDA_IMAGE=shogo82148/lambda-python:3.12.2024.10.18 uv run pytest