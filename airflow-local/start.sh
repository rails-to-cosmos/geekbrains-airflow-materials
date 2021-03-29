#!/usr/bin/env bash

airflow db init
airflow scheduler & airflow webserver -p 8080 && fg
