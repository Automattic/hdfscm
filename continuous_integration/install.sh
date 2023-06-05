#!/usr/bin/env bash
set -xe

pip install -U pip
pip install notebook pyarrow pytest nose flake8

cd ~/hdfscm
pip install -v --no-deps .

pip list
