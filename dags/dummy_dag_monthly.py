from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 8, 1),
    "owner": "ops",
}

# Initialize the DAG
with DAG(
    dag_id='dummy_dag_monthly',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
) as dag:

    task1 = EmptyOperator(
        task_id='task1',
    )

    task2 = EmptyOperator(
        task_id='task2',
    )

    with TaskGroup('taskgroup1') as taskgroup1:
        task1_1 = EmptyOperator(
            task_id='task1_1',
        )
        task1_2 = EmptyOperator(
            task_id='task1_2',
        )
        task1_1 >> task1_2

    with TaskGroup('taskgroup2') as taskgroup2:
        task2_1 = EmptyOperator(
            task_id='task2_1',
        )
        task2_2 = EmptyOperator(
            task_id='task2_2',
        )
        task2_1 >> task2_2

    final_task = EmptyOperator(
        task_id='final_task',
    )

    task1 >> task2 >> [taskgroup1, taskgroup2] >> final_task
