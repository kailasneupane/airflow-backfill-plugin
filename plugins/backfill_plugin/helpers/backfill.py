import io
import logging
import subprocess
import traceback
import pendulum
from airflow.utils.session import create_session
from backfill_plugin.models.backfill_dag_submission_model import (
    BackfillDagSubmissionModel,
)
from backfill_plugin.helpers import dag as dag_helper

logger = logging.getLogger("backfill_logger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")


def initiate_backfill_steps(
    dag_id, tasks, start_date, end_date, ignore_dependencies, submission_id
):
    start_datetime, end_datetime = _normalize_start_and_end_datetime_for_backfill(
        dag_helper.get_schedule_of_a_dag(dag_id), start_date, end_date
    )

    with create_session() as session:
        submission = session.query(BackfillDagSubmissionModel).get(submission_id)
        log_capture_string = io.StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setFormatter(formatter)
        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)
        try:
            logger.info(
                f"dag_id: {dag_id}, {'tasks: ' + str(tasks) + ', ' if tasks else ''}start_date: {start_date}, normalized_start_datetime:{start_datetime}, end_date: {end_date}, normalized_end_datetime:{end_datetime} {', ignore_dependencies: ' + str(ignore_dependencies) if tasks else ''}"
            )
            backfill_command = _build_backfill_command(
                dag_id, start_datetime, end_datetime, tasks, ignore_dependencies
            )
            logger.info(f"Executing command: {backfill_command}")
            submission.logs = log_capture_string.getvalue()
            session.commit()
            status = _run_backfill_as_command(backfill_command)
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


def _normalize_start_and_end_datetime_for_backfill(schedule_of_dag, start_date, end_date):
    """
    It returns modified start and end date to handle one of airflow's quirks.
    This function will apply start_dateT<scheduleTime> on start_date, and end_dateT<scheduleTime minus 1 minute> on
    end_date so that the backfill run on data_interval_end date will not execute.
    """

    if schedule_of_dag.startswith("@"):
        start_date_time = pendulum.parse(start_date).format("YYYY-MM-DDTHH:mm:ss")
        end_date_time = (
            pendulum.parse(end_date).subtract(minutes=1).format("YYYY-MM-DDTHH:mm:ss")
        )
    else:
        minute, hour, *_ = schedule_of_dag.split()
        start_date_time = pendulum.parse(start_date).format(
            f"YYYY-MM-DDT{hour.zfill(2)}:{minute.zfill(2)}:ss"
        )
        end_date_time = (
            pendulum.parse(f"{end_date}T{hour.zfill(2)}:{minute.zfill(2)}")
            .subtract(minutes=1)
            .format("YYYY-MM-DDTHH:mm:ss")
        )
    return start_date_time, end_date_time


def _build_backfill_command(dag_id, start_date, end_date, tasks, ignore_dependencies):
    executor = "airflow"
    command = [
        executor,
        "dags",
        "backfill",
        dag_id,
        "-s",
        f'"{start_date}"',
        "-e",
        f'"{end_date}"',
        "--reset-dagruns",
        "-y",
    ]
    if tasks:
        command.extend(["-t", f'"{"|".join(tasks)}"'])
        if ignore_dependencies:
            command.append("-i")
    return " ".join(command)


def _run_backfill_as_command(command):
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            executable="/bin/bash",
            text=True,
        )
        # logger.info(f"Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Command '{e.cmd}' failed with return code {e.returncode}")
        if "airflow.exceptions" in e.stderr:
            logger.error(
                f"airflow.exceptions{e.stderr.split('airflow.exceptions')[-1][-60000:]}"
            )
        else:
            logger.error(f"Error output: {e.stderr[-60000:]}")
        return 0  # failed
    return 1  # passed
