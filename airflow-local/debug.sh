#!/usr/bin/env bash

export AIRFLOW_HOME=$PWD

airflow tasks test $@
