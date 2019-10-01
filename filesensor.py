from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from sftp_sensor import SFTPSensor
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.dummy_operator import DummyOperator

# Constants
FILENAME = 'test.txt'
FILEPATH = '/home/bhudson/sftp_inbound/'
FILEPATTERN = r'^test.*\.txt$'
SFTP_CONN_ID = 'sftp_test'

TASKNAME = 'poke_for_file'

args = {
    'start_date': datetime(2019,9,25,0,0,0),
    'email': [],
    'depends_on_past': False,
    'email_on_failure': True,
    'provide_context': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds = 30),
}



dag = DAG(
    'SFTP_Sensor_Test',
    schedule_interval = None,
    default_args = args,
    catchup = False,
    max_active_runs = 1,
    concurrency = 1,
)

check_file_sensor = SFTPSensor(
    task_id = TASKNAME,
    filepath = FILEPATH,
    filepattern = FILEPATTERN,
    sftp_conn_id = SFTP_CONN_ID,
    poke_interval = 1,
    dag = dag
)

def process_file(**context):
    file_to_process = context['task_instance'].xcom_pull(
        key = 'file_name', task_ids = TASKNAME)
    logging.info('Filename please: {}'.format(file_to_process))

process_task = PythonOperator(
    task_id = 'list_file',
    python_callable = process_file,
    dag = dag
)

archive_task = 

dummy = DummyOperator(
    task_id = 'last_task',
    dag = dag,
)

check_file_sensor >> process_task >> dummy