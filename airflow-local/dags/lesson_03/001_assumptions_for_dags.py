from airflow.models import DAG
from util.settings import default_settings


with DAG(**default_settings()) as dag:
    pass
