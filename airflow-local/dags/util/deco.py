# Airflow decorators
# Enables operators/sensors/pipelines definitions in functional style:

# @python_operator(<operator default arguments>)
# def custom_operator_implementation(<operator arguments>):
#     pass

import inspect
from copy import copy

from airflow import settings
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.trigger_rule import TriggerRule

from util.tasks import get_leafs


def python_operator(**defaults):
    def operator_wrapper(python_callable):
        signature = inspect.signature(python_callable)

        def operator_constructor(**kwargs):
            defaults['task_id'] = kwargs.pop('task_id', None) or defaults.get('task_id') or python_callable.__name__

            try:
                cmdag = settings.CONTEXT_MANAGER_DAG
            except AttributeError:  # Airflow 2.0+
                from airflow.models.dag import DagContext
                cmdag = DagContext.get_current_dag()

            dag = kwargs.get('dag', None) or defaults.get('dag', None) or cmdag
            dag_args = copy(dag.default_args) if dag else {}
            dag_params = copy(dag.params) if dag else {}
            default_args = {}

            if 'default_args' in defaults:
                default_args = defaults['default_args']
                if 'params' in default_args:
                    dag_params.update(default_args['params'])
                    del default_args['params']

            dag_args.update(default_args)
            default_args = dag_args

            for arg in signature.parameters:
                if arg not in kwargs and arg in default_args:
                    kwargs[arg] = default_args[arg]

            return PythonOperator(
                python_callable=python_callable,
                op_kwargs=kwargs,
                params=dag_params,
                **defaults,
            )
        return operator_constructor
    return operator_wrapper


def short_circuit(**defaults):
    def operator_wrapper(python_callable):
        signature = inspect.signature(python_callable)

        def operator_constructor(**kwargs):
            defaults['task_id'] = kwargs.pop('task_id', None) or defaults.get('task_id') or python_callable.__name__
            dag = kwargs.get('dag', None) or defaults.get('dag', None) or settings.CONTEXT_MANAGER_DAG
            dag_args = copy(dag.default_args) if dag else {}
            dag_params = copy(dag.params) if dag else {}
            default_args = {}

            if 'default_args' in defaults:
                default_args = defaults['default_args']
                if 'params' in default_args:
                    dag_params.update(default_args['params'])
                    del default_args['params']

            dag_args.update(default_args)
            default_args = dag_args

            for arg in signature.parameters:
                if arg not in kwargs and arg in default_args:
                    kwargs[arg] = default_args[arg]

            return ShortCircuitOperator(
                python_callable=python_callable,
                op_kwargs=kwargs,
                params=dag_params,
                **defaults,
            )
        return operator_constructor
    return operator_wrapper


def python_sensor(**defaults):
    def sensor_wrapper(python_callable):
        signature = inspect.signature(python_callable)

        def sensor_constructor(**kwargs):
            defaults['task_id'] = kwargs.pop('task_id', None) or defaults.get('task_id') or python_callable.__name__
            dag = kwargs.get('dag', None) or defaults.get('dag', None) or settings.CONTEXT_MANAGER_DAG
            dag_args = copy(dag.default_args) if dag else {}
            dag_params = copy(dag.params) if dag else {}
            default_args = {}

            if 'default_args' in defaults:
                default_args = defaults['default_args']
                if 'params' in default_args:
                    dag_params.update(default_args['params'])
                    del default_args['params']

            dag_args.update(default_args)
            default_args = dag_args

            for arg in signature.parameters:
                if arg not in kwargs and arg in default_args:
                    kwargs[arg] = default_args[arg]

            return PythonSensor(
                python_callable=python_callable,
                op_kwargs=kwargs,
                params=dag_params,
                **defaults,
            )
        return sensor_constructor
    return sensor_wrapper