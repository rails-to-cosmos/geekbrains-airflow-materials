import logging
import time

from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook

from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def connection_operator(**context):
    mssql_hook = BaseHook.get_hook('airflow')
    hook.get_records('SELECT * FROM connection')


with DAG(**default_settings()) as dag:
    connection_operator()
