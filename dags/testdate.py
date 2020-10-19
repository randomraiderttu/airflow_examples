from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'start_date': datetime(2020,2,21,0,0,0),
    'email': ['bhudson@brierley.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds = 20)
}

dag = DAG(
    'Test_for_Start_Date_And_Intervals',
    schedule_interval = '*/5 * * * *',
    default_args = args,
    catchup = False,
)

def test_function():
    print('Testing a failure')
    print('Hey, I am doing some cool functionality.')

    return True


with dag:
    dummy1 = DummyOperator(
        task_id = 'check_for_file',
        dag = dag
    )

    dummy2 = PythonOperator(
        task_id = 'last_task',
        python_callable = test_function,
        dag = dag
    )

dummy1 >> dummy2