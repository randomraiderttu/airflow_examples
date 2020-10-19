## Default args are passed to all operators. The operator can override a value in the default args if it's explicitly set in the Operator.
## Base Operator Default Arg Possibilities
# args = {
#     'task_id': ,                        # a unique meaningful string for the task.
#     'owner ': ,                         # Owner of the task, using the unix username is recommended
#     'email': ,                          # List of emails separated by comma (or pass in a python list [])
#     'email_on_retry': ,                 # Send email for a task that is getting retried
#     'email on_failure': ,               # Send email for a task that is failed
#     'retries': ,                        # The nubmer of retries to attempt before failing the task
#     'retry_delay': ,                    # delay between retries (datetime.timedelta())
#     'retry_exponential_backoff': ,      # Allow progressingly longer waits between retries. Boolean
#     'max_retry_delay': ,                # Maximum delay between retries
#     'start_date': ,                     # Start date of the task
#     'end_date': ,                       # End date for a task - no execution after that date
#     'depends_on_past': ,                # Tasks must rely on the previous task's schedule to be successful before executing
#     'wait_for_downstream': ,            # When set to true, an instance of task X will wait for tasks immediately downstream of the previous instance to be successful
#     'queue': ,                          # Which queue to target when running the job
#     'dag': ,                            # a reference to the dag th etask is attached to
#     'priority_weight': ,                # Priority weight of this task against other tasks.  Allos the executer to trigger higher priority tasks before others when things get backed up.
#     'weight_rule': ,                    # Weighting method used for the effective total weight of a task. OPtions oare downstream, upstream, and absolute.
#     'pool': ,                           # the slot pool this task should run in. Slot pools are a way to limit concurrency
#     'pool_slots': ,                     # the number of pool slots this task should use
#     'sla': ,                            # datetime.timedelta() - time by which the job is expected to succeed.
#     'execution_timeout': ,              # max time allowed for the execution of a task instance before it's set to failed.
#     'on_failure_callback': ,            # function to be called when a task instance fails.
#     'on_execute_callback': ,            # function to be called right before the task is executed.
#     'on_retry_callback': ,              # Like other callbacks except its called when a retry happens
#     'on_success_callback': ,            # Like other callbacks - done when task succeeds
#     'trigger_rule': ,                   # defines the rule by which dependencies are applied for the task to get triggered. (Options: all _success','all_failed','all_done','dummy')
#     'resources': ,                      # A map of resource parameter names and their values.
#     'run_as_user': ,                    # Unix username to impoersonate while running tasks.
#     'task_concurrency': ,               # When set, a task will be able to limit the concurrent runs across execution dates.
#     'executor_config': ,                # Additional task_level configuration parameters.
#     'do_xcom_push': ,                   # If True, the xcom is pushed to the containing Operator's result.
# }

# dag = DAG(
#         dag_id = ,                # ID of the DAG
#         description = ,           # Name that shows up on dashboard for DAG
#         schedule_interval = ,     # Cron-style schedule interval
#         start_date = ,            # Timestamp it will use to attempt to backfill
#         end_date = ,              # A hard stop date that can be optionally applied - DAG can't run past this date
#         full_filepath = ,         # No idea
#         template_searchpath = ,   # List of folders that defines where jinja will look for templates
#         template_undefined = ,    # No idea - documentation says "Template Undefined Type"
#         user_defined_macros = ,   # Dict of macros that will be exposed to your jinja templates
#         user_defined_filters = ,  # Dict of filters that will be exposed to jinja templates
#         default_args = ,          # Dict of default parameters to be used as a constructor keyword parameters when initializing operators
#         concurrency = ,           # Number of tasks instances allowed to run concurrently
#         max_active_runs = ,       # Max number of active DAG runs.  Beyond that, the scheduler won't create new DAGs runs.
#         dagrun_timeout = ,        # HOw long a DagRun should be up for timing out/failing. Only applies to scheduled dagruns.
#         sla_miss_callback = ,     # specify a dunction to call when reporting SLA timeouts
#         default_view = ,          # Specifi DAG default view (tree, graph, duration, gantt, landing_times)
#         orientation = ,           # Specify DAG orientation in the graph view (Left-Right (LR), Top-Bottom (TB), Right-Left (RL), Bottom-Top(BT)
#         catchup = ,               # Perform scheduler catchup (or only run latest). Defaults to True
#         on_success_callback = ,   # Run specified function when DAG succeeds
#         on_failure_callback = ,   # Run specified function when DAG fails
#         doc_md = ,                # No Idea
#         params = ,                # Dict of DAG level parameters that are made accessible in templates. Can be overrident at task level.
#         access_control = ,        # Dict that can specify option DAG-level permissions {'role1': {'can_dag_read'}, 'role2': {'can_dag_edit'}}}
#         is_paused_upon_creation = # specifies if the DAG will be paused on initial creation or not. Ignored if DAG already exists.
#         )
from datetime import datetime, timedelta
import time
import logging
import os
import re
import string
import random
import csv

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag # Needed to trigger dags
from airflow.utils import timezone # Needed to determine utc in iso format for passing the run_id to the triggered dag.

#########################################################################

args = {
    'start_date': datetime(2020,2,20,0,0,0),
    'email': ['bhudson@brierley.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'provide_context': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds = 30),
    'execution_timeout': timedelta(minutes = 2),
}

dag = DAG(
    dag_id = 'Create_Test_Store_File',
    description = 'Create Test Store File',
    default_args = args,
    max_active_runs = 1,
    concurrency = 30,
    catchup = False,
    schedule_interval = None,
)

#########################################################################

def getRandomString(stringLength=10):
    """
    Obtain a random string of a lowercase letters for the length passed in.

    Keyword Arguments:
        stringLength {int} -- Create a string of provided length or use default (default: {10})
    """
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for x in range(stringLength))


def createLocalTestFile(**kwargs):
    """
    Create a test file locally.
    """

    loadDirectory = '/usr/local/airflow/test_dir'
    destDirectory = '/airflow_test/postgresdb_etl_poc_flatfiles/auto'
    fileName = 'BP_STORE_{}.txt'.format(datetime.now().strftime(('%Y%m%d%H%M%S')))
    fullPath = os.path.join(loadDirectory, fileName)
    streetNames = ['Main St',
        'Washington Ave',
        'Dover Ln',
        'Marshall Pkwy',
        'Pinecone Dr',
        'Corona Wy']
    cityNames = ['Dallas', 'Lubbock', 'Denver', 'Raleigh', 'Tallahassee', 'Bozeman']
    states = ['TX', 'CA', 'WA', 'AL', 'NC', 'NE']
    statuses = ['OPEN','CLOSE','OPEN','OPEN','OPEN','OPEN']

    rowList = []

    for i in range(random.randint(4,20)):
        tempDict = {}
        tempDict['storeNumber'] = random.randint(1000,1100)
        tempDict['storeName'] = 'Store {}'.format(getRandomString(10))
        tempDict['storeAddress1'] = '{} {}'.format(random.randint(1,9999),random.choice(streetNames)) 
        tempDict['storeAddress2'] = None
        tempDict['storeCity'] = random.choice(cityNames)
        tempDict['storeState'] = random.choice(states)
        tempDict['storeZip'] = '{}-{}'.format(str(random.randint(0,99999)).zfill(5),str(random.randint(0,9999)).zfill(4))
        tempDict['storeStatus'] = random.choice(statuses)
        rowList.append(tempDict)

    csv_columns = [
        'storeNumber',
        'storeName',
        'storeAddress1',
        'storeAddress2',
        'storeCity',
        'storeState',
        'storeZip',
        'storeStatus'
        ]

    try:
        with open(fullPath, 'w') as f:
            writer = csv.DictWriter(f, fieldnames = csv_columns)
            writer.writeheader()
            for data in rowList:
                writer.writerow(data)

        print('')
    except IOError:
        print('Issue writing to file')
        raise IOError

    sourceHook = SFTPHook(ftp_conn_id = 'kub2VM')

    try:
        sourceHook.store_file(os.path.join(destDirectory, fileName), fullPath)
    except:
        logging.error('Trouble with the store_file step. SourceDir: {} -- DestDir: {}'.format(os.path.join(destDirectory, fileName), fullPath))
    finally:
        if sourceHook.conn:
            sourceHook.close_conn()

task1 = PythonOperator(
    task_id = 'Create_and_Move_File',
    provide_context = True,
    python_callable = createLocalTestFile,
    dag = dag
)