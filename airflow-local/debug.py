#!/usr/bin/env python

"""
Press [Tab] to complete the current word.
- The first Tab press fills in the common part of all completions
    and shows all the completions. (In the menu)
- Any following tab press cycles through all the possible completions.
"""

import os
import sys
import datetime
import json
import subprocess

from prompt_toolkit.completion import FuzzyWordCompleter
from prompt_toolkit.shortcuts import prompt

import diskcache

os.environ["AIRFLOW_HOME"] = os.path.dirname(os.path.realpath(__file__))

import airflow
from airflow.models import DagBag
dagbag = DagBag(airflow.configuration.conf.get('core', 'DAGS_FOLDER'))

dag_ids = set(dagbag.dag_ids)
dag_tasks = {
    dag_id: [task.task_id for task in dagbag.get_dag(dag_id).tasks]
    for dag_id in dagbag.dag_ids
}

dag_compl = FuzzyWordCompleter(dag_ids)
task_compl = {dag_id: FuzzyWordCompleter(tasks) for dag_id, tasks in dag_tasks.items()}


def main():
    try:
        with open('/tmp/.airflow_debug_history', 'r') as f:
            defaults = json.load(f)
    except IOError:
        defaults = {}

    if '--no-ask' in sys.argv:
        dag = defaults.get('dag', '')
        task = defaults.get('task', '')
        date = defaults.get('date', datetime.datetime.now().strftime('%Y-%m-%dT%H:%m'))
    else:
        dag = prompt(
            "DAG: ",
            default=defaults.get('dag', ''),
            completer=dag_compl,
            complete_while_typing=True,
        )

        task = prompt(
            "Task: ",
            default=defaults.get('task', ''),
            completer=task_compl[dag] if dag in task_compl else None,
            complete_while_typing=True,
        )

        date = prompt(
            "Date: ",
            default=defaults.get('date', datetime.datetime.now().strftime('%Y-%m-%d')),
        )

    with open('/tmp/.airflow_debug_history', 'w') as f:
        json.dump({
            'dag': dag,
            'task': task,
            'date': date,
        }, f)

    subprocess.call(['airflow', 'tasks', 'test', dag, task, date])


if __name__ == "__main__":
    main()
