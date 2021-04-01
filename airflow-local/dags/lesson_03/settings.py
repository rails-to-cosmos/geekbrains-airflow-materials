import os
import datetime as dt


settings = {
    'dag_id': 'titanic_pivot',  # Имя DAG
    'schedule_interval': '@daily',  # Периодичность запуска, например, "00 15 * * *"
    'default_args': {  # Базовые аргументы для каждого оператора
        'owner': 'airflow',  # Информация о владельце DAG
        'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
        'retries': 1,  # Количество повторений в случае неудач
        'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
        'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
    }
}
