#!/usr/bin/env bash

export AIRFLOW_HOME=$PWD

airflow dags backfill $@
