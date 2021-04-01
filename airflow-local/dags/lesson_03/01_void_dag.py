from airflow.models import DAG
from lesson_03.settings import default_settings
from util.deco import python_operator


@python_operator()
def hello_world(**_):
    print("Hey")


with DAG(**default_settings()) as dag:
    hello_world()
