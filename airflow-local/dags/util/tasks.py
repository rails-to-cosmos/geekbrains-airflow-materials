import airflow
import datetime
import itertools
import logging

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Union, Generator

from airflow.models import BaseOperator
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State


def set_task_state(operator, state, context, session):
    logging.info('Set task %s state to %s', operator.task_id, state)
    now = timezone.utcnow()
    ti = TI(operator, execution_date=context['ti'].execution_date)
    ti.state = state
    ti.start_date = now
    ti.end_date = now
    session.merge(ti)


def group_tasks(tasks, method) -> Dict:
    grouped_tasks = itertools.groupby(tasks, method)
    result = defaultdict(list)
    for key, ts in grouped_tasks:
        for task in ts:
            result[key].append(task)
    return result


class TaskCategory:
    ANALYTICS_DASHBOARDS = 'analytics_dashboards'
    ANALYTICS_DASHBOARDS_REPORT = 'analytics_dashboards_report'
    ANALYTICS_METRICS = 'analytics_metrics'
    ANALYTICS_TABLES = 'analytics_tables'
    ANALYTICS_TESTS = 'analytics_tests'
    GOOGLE_ANALYTICS = 'google_analytics'

    all_categories = (
        ANALYTICS_DASHBOARDS,
        ANALYTICS_DASHBOARDS_REPORT,
        ANALYTICS_METRICS,
        ANALYTICS_TABLES,
        ANALYTICS_TESTS,
        GOOGLE_ANALYTICS,
    )

    priority_table = (
        GOOGLE_ANALYTICS,
        ANALYTICS_TABLES,
        ANALYTICS_METRICS,
        ANALYTICS_TESTS,
        ANALYTICS_DASHBOARDS,
        ANALYTICS_DASHBOARDS_REPORT,
    )


@dataclass(frozen=True)
class AirflowTask:
    id: str
    type: str
    dag: airflow.DAG
    instance: TI
    operator: BaseOperator

    @property
    def state_localized(self):
        return {
            State.NONE: 'Задача не запускалась',
            State.REMOVED: 'Задача удалена',
            State.SCHEDULED: 'Задача запланирована',
            State.QUEUED: 'Задача в очереди',
            State.RUNNING: 'Задача выполняется',
            State.SUCCESS: 'Успех',
            State.SHUTDOWN: 'Задача зависла (на помощь!)',
            State.FAILED: 'Ошибка',
            State.UP_FOR_RETRY: 'Задача в очереди (были неуспешные попытки)',
            State.UP_FOR_RESCHEDULE: 'Пассивное наблюдение',
            State.SKIPPED: 'Игнор',
        }[self.instance.state]


def get_children(root: BaseOperator) -> List[BaseOperator]:
    result = [root]
    for ds in root.downstream_list:
        children = get_children(ds)
        result.extend(children)
    return list(set(result))


def collect_children_preserve_order(root: BaseOperator) -> Generator[BaseOperator, None, None]:
    yield root
    for child in root.downstream_list:
        yield from collect_children_preserve_order(child)


def get_parents(leaf: Union[BaseOperator, List[BaseOperator]]) -> List[BaseOperator]:
    leaves: List[BaseOperator] = []

    if isinstance(leaf, BaseOperator):
        leaves.append(leaf)
    else:
        leaves.extend(leaf)

    result: List[BaseOperator] = []
    for l in leaves:
        if not l.upstream_list:
            result.append(l)
        else:
            for p in get_parents(l.upstream_list):
                if p not in result:
                    result.append(p)

    return result


def get_upstream(leaf: Union[BaseOperator, List[BaseOperator]]) -> List[BaseOperator]:
    leaves: List[BaseOperator] = []

    if isinstance(leaf, BaseOperator):
        leaves.append(leaf)
    else:
        leaves.extend(leaf)

    result: List[BaseOperator] = []
    for l in leaves:
        result.append(l)
        for p in get_upstream(l.upstream_list):
            if p not in result:
                result.append(p)
    return result


def get_parent(leaf: Union[BaseOperator, List[BaseOperator]]) -> BaseOperator:
    parents = get_parents(leaf)
    assert len(parents) == 1
    return parents[0]


def get_leafs(root: BaseOperator) -> List[BaseOperator]:
    result = []
    for ds in root.downstream_list:
        children = get_leafs(ds)
        result.extend(children)
    return list(set(result)) or [root]


@provide_session
def past_downstream_finished(operator: BaseOperator,
                             execution_date: datetime.datetime,
                             session=None) -> bool:
    '''Actually this is a trait for CronSensor.

    Используйте, когда нужно зависеть от прошлых выполнений не только
    конкретного оператора, но и всего даунстрима.

    Сильно отличается от настройки `depends_on_past`:

    - при `depends_on_past` оператор не стартует, если нет прошлых
      выполнений. А этот стартует.

    - при `depends_on_past` оператор не стартует, если прошлые
      выполнения оператора завершились с ошибкой.

    - `past_downstream_finished` ждет, пока закончится предыдущее
      выполнение всего даунстрима, и стартует текущий. При этом не
      важно, с ошибкой ли завершился прошлый даунстрим.

    Посмотрите примеры использования в реализации CronSensor.
    '''

    child_ids = [op.task_id for op in get_children(operator)]
    logging.info('Past tasks: %s', child_ids)

    tis = session.query(TI).filter(
        TI.dag_id == operator.dag.dag_id,
        TI.task_id.in_(child_ids),
        TI.state.in_(State.unfinished()),
        TI.execution_date < execution_date,
    ).all()

    if len(tis) > 0:
        logging.info('Waiting for previous pipelines to finish... (%s)', tis)

    return len(tis) == 0
