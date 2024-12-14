import io
import logging
import subprocess
import traceback
from pathlib import Path
from airflow.models import DagBag
from airflow.utils.session import create_session
from backfill_plugin.models.backfill_dag_submission_model import BackfillDagSubmissionModel

logger = logging.getLogger('backfill_logger')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

def initiate_backfill_steps(dag_id, tasks, start_date, end_date, ignore_dependencies, submission_id):
    with create_session() as session:
        submission = session.query(BackfillDagSubmissionModel).get(submission_id)
        log_capture_string = io.StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        try:
            backfill_command = build_backfill_command(dag_id, start_date, end_date, tasks, ignore_dependencies)
            logger.info(f"dag_id: {dag_id}, {'tasks: ' + str(tasks) + ', ' if tasks else ''}start_date: {start_date}, end_date: {end_date} {', ignore_dependencies: ' + str(ignore_dependencies) if tasks else ''}")
            logger.info(f"Executing command: {backfill_command}")
            submission.logs = log_capture_string.getvalue()
            session.commit()
            status = run_backfill_as_command(backfill_command)
            submission.logs = log_capture_string.getvalue()
            session.commit()
        except Exception:
            logger.info(traceback.format_exc())
            status = 0

        logger.info(f"Backfill status: {'Failed' if status == 0 else 'Successful'}")
        logger.removeHandler(ch)
        submission.logs = log_capture_string.getvalue()
        submission.status = status
        session.commit()

def build_backfill_command(dag_id, start_date, end_date, tasks, ignore_dependencies):
    if Path("~/airflow/.venvs/2.9.3/bin/airflow").expanduser().exists():
        executor = str(Path("~/airflow/.venvs/2.9.3/bin/airflow").expanduser()) #for reflected prod airflow
    else:
        executor = "airflow" #for local airflow
    command = [executor, 'dags', 'backfill', dag_id, '-s', start_date, '-e', end_date, '--reset-dagruns', '-y']
    if tasks:
        command.extend(['-t', f"\"{'|'.join(tasks)}\""])
        if ignore_dependencies:
            command.append('-i')
    return ' '.join(command)

def run_backfill_as_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        #logger.info(f"Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command '{e.cmd}' failed with return code {e.returncode}")
        if "airflow.exceptions" in e.stderr:
            logger.error(f"airflow.exceptions{e.stderr.split('airflow.exceptions')[-1][-60000:]}")
        else:
            logger.error(f"Error output: {e.stderr[-60000:]}")
        return 0 #failed
    return 1 #passed


def get_active_dags_and_tasks():
    dag_bag = DagBag()
    active_dags = {dag.dag_id: [task.task_id for task in dag.tasks] for dag in dag_bag.dags.values() if
                   not dag.is_paused and not dag.schedule_interval is None }
    return active_dags
