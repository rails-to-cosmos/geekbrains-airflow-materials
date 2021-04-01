from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from util.settings import default_settings


def python_operator_task(**context):
    print('Hey world, I\'m a classic python operator!')


with DAG(**default_settings()) as dag:
    classic_python_operator_definition = PythonOperator(
        task_id='classic_python_operator',
        python_callable=python_operator_task,
        dag=dag,
    )
