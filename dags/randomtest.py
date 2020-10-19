from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

#########################################################################

args = {
    'start_date': datetime(2020,2,20,0,0,0),
}

dag = DAG(
    dag_id = 'A_Random_Test',
    description = 'Testing',
    default_args = args,
    catchup = False,
    schedule_interval = None,
)

filename1 = f'testfile_{datetime.now()}.txt'

task1 = BashOperator(
    task_id='test1',
    bash_command=f'echo My-File: {filename1}',
    dag=dag
    )

task2 = BashOperator(
    task_id='test2',
    bash_command=f'echo My-File: {filename1}',
    dag=dag
    )

[task1, task2]