from airflow.models import DAG
from lesson_03.settings import settings


with DAG(**settings) as dag:
    pass
