import json
from threading import Thread
from flask import Blueprint, request, redirect, url_for
from flask_login import current_user
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from backfill_plugin.models.backfill_dag_submission_model import BackfillDagSubmissionModel
from airflow import settings
from airflow.plugins_manager import AirflowPlugin
import datetime
from backfill_plugin.helpers import backfill_helper
#from backfill_plugin.tasks import backfill_task

my_blueprint = Blueprint(
    "backfill_plugin_blueprint",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/lets_do_backfill/static"
)

engine = create_engine(settings.SQL_ALCHEMY_CONN)
Session = sessionmaker(bind=engine)

class lets_do_backfill(AppBuilderBaseView):
    default_view = "backfill_view"

    @expose("/", methods=["GET", "POST"])
    def backfill_view(self):

        if not current_user.is_authenticated:
            return redirect(url_for('routes.index'))


        session = Session()
        error_message = None
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        if request.method == "POST":
            dag = request.form.get("dag")
            enable_tasks = request.form.get("enable_tasks") == "on"
            tasks = request.form.getlist("tasks") if enable_tasks else []
            ignore_dependencies = request.form.get("ignore_dependencies") == "on" and len(tasks) > 0
            start_date = request.form.get("start_date")
            end_date = request.form.get("end_date")

            if start_date > end_date or end_date > current_date or start_date > current_date:
                error_message = "Start Date must be earlier than End Date and dates must not exceed current date."

            else:
                # Save to database
                submission = BackfillDagSubmissionModel(dag=dag, start_date=start_date, end_date=end_date, tasks=json.dumps(tasks), ignore_dependencies=ignore_dependencies, status=-1)
                session.add(submission)
                session.commit()

                #Run the initiate_backfill_steps() in a separate thread
                # logging.info("Task submitted to a separate thread")
                thread = Thread(target=backfill_helper.initiate_backfill_steps, args=(dag, tasks, start_date, end_date, ignore_dependencies, submission.id))
                thread.start()

                #Call the Celery task
                # logging.info("Task submitted to celery")
                # backfill_task.apply_async(args=[dag, tasks, start_date, end_date, ignore_dependencies, submission.id])

                #Redirect to avoid form resubmission
                return redirect(url_for('lets_do_backfill.backfill_view'))

        # Retrieve all submissions
        submissions = session.query(BackfillDagSubmissionModel).order_by(BackfillDagSubmissionModel.timestamp.desc()).limit(10).all()

        return self.render_template("main.html",
                                    submissions=submissions,
                                    dags_and_tasks=backfill_helper.get_active_dags_and_tasks(),
                                    default_start_date=current_date,
                                    default_end_date=current_date,
                                    error_message=error_message)


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
