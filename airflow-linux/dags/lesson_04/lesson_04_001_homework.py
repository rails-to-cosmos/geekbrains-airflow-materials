from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from util.helpers import make_folder
from util.settings import default_settings
from util.titanic import (
    pivot_dataset,
    mean_fare_per_class,
    download_titanic_dataset,
)


# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**default_settings()) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )

    create_titanic_folder = PythonOperator(
        task_id='make_directory',
        python_callable=make_folder
    )

    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )

    # В конец пайплайна (после завершения тасок pivot_titanic_dataset и mean_fares_titanic_dataset) добавить шаг
    # с названием last_task, на котором в STDOUT выводится строка, сообщающая об окончании расчета и выводящая
    # execution date в формате YYYY-MM-DD. Пример строки: "Pipeline finished! Execution date is 2020-12-28"
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"'
    )

    # Порядок выполнения тасок
    mean_fares_titanic_dataset = mean_fare_per_class()
    first_task >> create_titanic_folder >> download_titanic_dataset() >> (pivot_titanic_dataset, mean_fares_titanic_dataset)
    (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task
