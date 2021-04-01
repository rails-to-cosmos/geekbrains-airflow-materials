#!/usr/bin/env bash

export AIRFLOW_HOME=$PWD

airflow scheduler & airflow webserver -p 8080 && fg
