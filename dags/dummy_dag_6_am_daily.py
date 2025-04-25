from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 8, 1),
    "owner": "ops",
}


def print_data_interval(**kwargs):
    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']

    print(f"Data Interval Start: {data_interval_start}")
    print(f"Data Interval End: {data_interval_end}")

def print_data_intervals(task_id):
    return PythonOperator(
    task_id=task_id,
    python_callable=print_data_interval,
    provide_context=True,
)

# Initialize the DAG
with DAG(
    dag_id='dummy_dag_6_am_daily',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

    task1 = print_data_intervals("task1")

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

    final_task = print_data_intervals("final_task")

    task1 >> task2 >> [taskgroup1, taskgroup2] >> final_task
