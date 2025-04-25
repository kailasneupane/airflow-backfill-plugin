import json
from threading import Thread
from flask import Blueprint, request, redirect, url_for
from flask_login import current_user
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from backfill_plugin.models.backfill_dag_submission_model import (
    BackfillDagSubmissionModel,
)
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
import datetime
from backfill_plugin.helpers import backfill as backfill_helper
from backfill_plugin.helpers import dag as dag_helper

EMPTY_FORM_DATA = {'dag': None, 'start_date': None,
                   'end_date': None,
                   'run_all_dependencies': None,
                   'enable_tasks': None}

my_blueprint = Blueprint(
    "backfill_plugin_blueprint",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/lets_do_backfill/static",
)


class lets_do_backfill(AppBuilderBaseView):
    default_view = "backfill_view"

    @expose("/", methods=["GET"])
    def backfill_view(self):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.index"))

        error_message = request.args.get("error_message")
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        with Session() as session:
            # Retrieve latest 10 submissions
            submissions = (
                session.query(BackfillDagSubmissionModel)
                .order_by(BackfillDagSubmissionModel.timestamp.desc())
                .limit(10)
                .all()
            )
            active_dags_and_tasks = dag_helper.get_active_dags_and_tasks(session)

        return self.render_template(
            "main.html",
            submissions=submissions,
            active_dags_and_tasks=active_dags_and_tasks,
            default_start_date=current_date,
            default_end_date=current_date,
            error_message=error_message,
            form_data=json.loads(request.args.get('form_data')) if error_message else EMPTY_FORM_DATA,
        )

    @expose("/", methods=["POST"])
    def submit_backfill(self):
        if not current_user.is_authenticated:
            return redirect(url_for("routes.index"))

        with Session() as session:
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")

            dag = request.form.get("dag")
            enable_tasks = request.form.get("enable_tasks") == "on"
            tasks = request.form.getlist("tasks") if enable_tasks else []
            ignore_dependencies = (
                request.form.get("run_all_dependencies") != "on" and len(tasks) > 0
            )
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")

            error_message = None
            if (
                start_date >= end_date
                or end_date > current_date
                or start_date > current_date
            ):
                error_message = "Start Date must be earlier than End Date and dates must not exceed current date."
            elif dag_helper.check_for_ongoing_dagrun(dag, session):
                error_message = f"Found an ongoing run for {dag}. Please stop it first or let it complete before proceeding."
            elif enable_tasks and len(tasks) == 0:
                error_message = f"Select specific tasks first or uncheck \"Select specific tasks\"."

            if error_message:
                form_data = {'dag':dag, 'start_date':start_date, 'end_date':end_date, 'run_all_dependencies':request.form.get("run_all_dependencies"), 'enable_tasks':request.form.get("enable_tasks")}
                return redirect(url_for("lets_do_backfill.backfill_view", error_message=error_message, form_data=json.dumps(form_data)))

            # Save backfill submission details to database
            submission = BackfillDagSubmissionModel(
                dag=dag,
                start_date=start_date,
                end_date=end_date,
                tasks=json.dumps(tasks),
                ignore_dependencies=ignore_dependencies,
                status=-1,
            )
            session.add(submission)
            session.commit()

            # Run the initiate_backfill_steps() in a separate thread
            # logging.info("Task submitted to a separate thread")
            thread = Thread(
                target=backfill_helper.initiate_backfill_steps,
                args=(
                    dag,
                    tasks,
                    start_date,
                    end_date,
                    ignore_dependencies,
                    submission.id,
                ),
            )
            thread.start()

        # Redirect to avoid form resubmission
        return redirect(url_for("lets_do_backfill.backfill_view"))


# Define the view in the Airflow UI
class MyViewPlugin(AirflowPlugin):
    name = "BackFiller Plugin"
    flask_blueprints = [my_blueprint]
    appbuilder_views = [
        {
            "name": "Goto backfill page",
            "category": "BackFiller",
            "view": lets_do_backfill,
        }
    ]
