from airflow.models import DAG, Variable
from util.settings import default_settings
from util.deco import python_operator


@python_operator()
def please_echo_my_variable(**_):
    try:
        test_variable = Variable.get("test_variable")
    except KeyError:
        print('My test variable is undefined')
    else:
        print('My test variable equals', test_variable)


with DAG(**default_settings()) as dag:
    please_echo_my_variable()
