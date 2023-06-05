#!/usr/bin/env bash
set -xe

cd hdfscm
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
py.test hdfscm --verbose
flake8 hdfscm
