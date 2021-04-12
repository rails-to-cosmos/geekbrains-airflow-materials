from airflow.operators.bash_operator import BashOperator


def bash(task_id, bash_command, **context):
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
    )
