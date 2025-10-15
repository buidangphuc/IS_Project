#!/usr/bin/env bash
set -e
export PYTHONPATH=src
uvicorn app:app --reload --host 0.0.0.0 --port 8000
