from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pandas as pd
from airflow.decorators import dag, task
from util.settings import default_settings
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}


@dag(**default_settings())
def hw_4_dag():
    """
    # Homework No.4 dag - same functionality as No.3 but
    realization with @dag and @task features
    """

    @task()
    def download_titanic_dataset(url='https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'):
        """
        # download_titanic_dataset task
        This task downloads the sample titanic dataset from predefined URL and returns
        {'titanic_df' : 'dataframe represented as json data in STR format'}
        It uses methodology of @task decorator covering all XCOM push / pull mechanics under the hood
        """
        df = pd.read_csv(url)
        return {'titanic_df_json_str': df.to_json()}

    @task()
    def pivot_dataset(titanic_df_json_str: dict):
        """
        # Pivot dataset task
        Takes the str of json data from XCOM,
        converts it to Pandas dataframe and makes some df aggregations
        Then dataframe is converted to the list of tuples and sent to external
        local DB
        """
        # преобразуем в pandas dataframe и изменяем агрегацией:

        titanic_df = pd.read_json(titanic_df_json_str['titanic_df_json_str'])
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()


        # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
        pg_hook = BaseHook.get_hook('postgres_default')

        # имя таблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
        pg_table_name = Variable.get('pivot_table_name')

        # перемалываем датафрейм в список кортежей, приводим типы к стандартным (str и int):
        pg_rows = list(df.to_records(index=False))
        pg_rows_conv = [(t[0], int(t[1]), int(t[2]), int(t[3])) for t in pg_rows]

        # извлекаем названия полей(колонок) датафрейма и приводим их типы к строковому:
        pg_columns = list(df.columns)
        pg_columns_conv = [pg_columns[0],
                           '"' + str(pg_columns[1]) + '"',
                           '"' + str(pg_columns[2]) + '"',
                           '"' + str(pg_columns[3]) + '"']

        # отправляем данные в локальную БД:
        pg_hook.insert_rows(table=pg_table_name,
                            rows=pg_rows_conv,
                            target_fields=pg_columns_conv,
                            commit_every=0,
                            replace=False)

    @task()
    def mean_fare_per_class(titanic_df_json_str: dict):
        """
        # Mean_fare_per_class task
        Takes the str of json data from XCOM,
        converts it to Pandas dataframe and makes some df aggregations
        Then dataframe is converted to the list of tuples and sent to external
        local DB
        """
        # преобразуем в pandas dataframe и изменяем группировками, агрегациями:
        titanic_df = pd.read_json(titanic_df_json_str['titanic_df_json_str'])
        df = titanic_df \
            .groupby(['Pclass']) \
            .agg({'Fare': 'mean'}) \
            .reset_index()

        # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
        pg_hook = BaseHook.get_hook('postgres_default')

        # имя тааблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
        pg_table_name = Variable.get('mean_fares_table_name')

        # перемалываем датафрейм в список кортежей, приводим типы к стандартным (int и float):
        pg_rows = list(df.to_records(index=False))
        pg_rows_conv = [(int(t[0]), float(t[1])) for t in pg_rows]

        # извлекаем названия полей(колонок) датафрейма:
        pg_columns = list(df.columns)

        # отправляем данные в локальную БД:
        pg_hook.insert_rows(table=pg_table_name,
                            rows=pg_rows_conv,
                            target_fields=pg_columns,
                            commit_every=0,
                            replace=False)

    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}"',
    )

    create_titanic_dataset = download_titanic_dataset()
    pivot_titanic_dataset = pivot_dataset(create_titanic_dataset)
    mean_fares_titanic_dataset = mean_fare_per_class(create_titanic_dataset)

    first_task >> create_titanic_dataset
    mean_fares_titanic_dataset >> last_task
    pivot_titanic_dataset >> last_task

my_titanic_dag = hw_4_dag()
