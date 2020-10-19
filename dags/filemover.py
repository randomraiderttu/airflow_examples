from datetime import datetime, timedelta
import logging
import json

from airflow import DAG
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Constants
FILENAME = 'test.txt'
FILEPATH = '/home/bhudson/sftp_inbound/'
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
    'sftp_listfiles',
    schedule_interval = '@daily',
    default_args = args,
    catchup = True,
)

def check_for_file_py(**kwargs):
    path = kwargs.get('path', None)
    logging.info('path type: {} || path value: {}'.format(type(path), path))
    sftp_conn_id = kwargs.get('sftp_conn_id', None)
    filename = kwargs.get('templates_dict').get('filename', None)
    sftp_hook = SFTPHook(ftp_conn_id = sftp_conn_id)
    logging.info('sftp_hook type: {} || sftp_hook value: {}'.format(type(sftp_hook), sftp_hook))
    sftp_client = sftp_hook.get_conn()
    fileList = sftp_hook.list_directory(FILEPATH)
    logging.info('FileList: {}'.format(fileList))
    if FILENAME in fileList:
        return True
    else:
        return False


with dag:
    check_file = ShortCircuitOperator(
        task_id = 'check_for_file',
        python_callable = check_for_file_py,
        templates_dict = {'filename': FILENAME},
        op_kwargs = {'path': FILEPATH,
                     'sftp_conn_id': SFTP_CONN_ID},
        provide_context = True,
    )

    dummy = DummyOperator(
        task_id = 'last_task',
    )

check_file >> dummy