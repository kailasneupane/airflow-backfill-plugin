# import logging
# from airflow.configuration import conf
# from celery import Celery
# from backfill_plugin.helpers import backfill_helper
#
# app = Celery(
#     'backfill_plugin',
#     broker=conf.get('celery', 'BROKER_URL'),
#     backend=conf.get('celery', 'RESULT_BACKEND')
# )
# @app.task
# def backfill_task(*args, **kwargs):
#     # Call the initiate_backfill_steps function
#     backfill_helper.initiate_backfill_steps(*args, **kwargs)

# run celery worker for backfill_task
# docker exec -it cdp-dbt-runner-eurema-airflow-worker-1 /bin/bash
# celery -A plugins.backfill_plugin.tasks worker --loglevel=debug

