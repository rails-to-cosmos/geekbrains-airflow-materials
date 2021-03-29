#!/usr/bin/env bash

export AIRFLOW_HOME=$PWD

pip install -r requirements.txt

airflow db init

airflow users create \
        --username admin \
        --password admin \
        --firstname Geek \
        --lastname Brain \
        --role Admin \
        --email email@address.com

airflow scheduler & airflow webserver -p 8080 && fg
