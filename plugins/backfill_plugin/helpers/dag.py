from airflow.models import DagBag, DagRun, DagModel, TaskInstance


def get_active_dags_and_tasks(session):
    active_dags = (
        session.query(DagModel.dag_id)
        .filter(DagModel.is_paused == False, DagModel.is_active == True)
        .all()
    )

    tasks_by_dag = {
        a_dag.dag_id: [
            task.task_id
            for task in session.query(TaskInstance.task_id)
            .filter(TaskInstance.dag_id == a_dag.dag_id)
            .distinct()
            .all()
        ]
        for a_dag in active_dags
    }

    return dict(sorted(tasks_by_dag.items()))  # sorted alphabetically


def check_for_ongoing_dagrun(dag_id, session):
    running_dag = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.state.in_(["running", "queued"])
    ).first()
    return running_dag is not None


def get_schedule_of_a_dag(dag_id):
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id)
    return dag.schedule_interval
