#!/usr/bin/env bash
set -e
export PYTHONPATH=src
python -m datascience.pipeline.streaming
