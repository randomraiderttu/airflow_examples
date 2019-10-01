from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Constants
FILENAME = 'test.txt'
FILEPATH = '/home/bhudson/sftp_inbound'
SFTP_CONN_ID = 'sftp_test'

args = {
    'start_date': datetime(2019,9,24,0,0,0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}

dag = DAG(
    'myDAG',
    schedule_interval = '@daily',
    default_args = args,
    catchup = True,
)


with dag:
    dummy1 = DummyOperator(
        task_id = 'first_task'
    )

    dummy2 = DummyOperator(
        task_id = 'last_task',
    )

dummy1 >> dummy2