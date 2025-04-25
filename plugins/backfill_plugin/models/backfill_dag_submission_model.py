from sqlalchemy import Column, Integer, String, DateTime, create_engine, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
import datetime
from airflow import settings

Base = declarative_base()


class BackfillDagSubmissionModel(Base):
    __tablename__ = "backfill_plugin_dag_submissions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    dag = Column(String(255), nullable=False)
    tasks = Column(Text)
    start_date = Column(String(255), nullable=False)
    end_date = Column(String(255), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    ignore_dependencies = Column(Boolean)
    logs = Column(Text)
    status = Column(Integer)


def create_table():
    engine = create_engine(settings.SQL_ALCHEMY_CONN)
    Base.metadata.create_all(engine)


# Create the table if it doesn't exist
create_table()
