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
import hashlib
from datetime import datetime, timedelta, timezone
import os
import logging
import gnupg
import pytz
from random import randint

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook

GPG_HOME = '/usr/local/airflow/.gnupg'


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
    dag_id = 'Create_SFTP_Test_File',
    description = 'CreateTestFile',
    default_args = args,
    max_active_runs = 1,
    concurrency = 1,
    catchup = False,
    schedule_interval = None,
)

dag_config = Variable.get('SFTP_Polling_JSON', deserialize_json = True)

##############################################################################################

def createTestFile(**kwargs):
    """
    Create a test file on one of the SFTP sites to initiate the transfer process
    """
    SFTP_Name = dag_config['SFTP_Polling_Sites'][0]['SFTP_Name']
    SFTP_Connection_Name = dag_config['SFTP_Polling_Sites'][0]['SFTP_Connection_Name']
    SFTP_Destination_Path = dag_config['SFTP_Polling_Sites'][0]['Feed_Groups'][0]['Feed_Group_Location']
    fileName = os.path.join(SFTP_Destination_Path, 'testfile_{}.txt'.format(randint(0, 9999999)))
    createFileCommand = "echo 'Hello World!' > {}".format(fileName)
    gpgCommand = "gpg --output {}.gpg -e -r bhudson@brierley.com {}".format(fileName, fileName)

    sftpHook = SFTPHook(ftp_conn_id = SFTP_Connection_Name)

    print('SFTP_Name: {}'.format(SFTP_Name))
    print('SFTP_Connection: {}'.format(SFTP_Connection_Name))
    print('SFTP_Destination_Path: {}'.format(SFTP_Destination_Path))
    print('Random Filename: {}'.format(fileName))
    print('GPG Command: {}'.format(gpgCommand))

    conn = sftpHook.get_conn()

    tempResults = conn.execute(createFileCommand)
    decodedString = [x.decode('utf-8') for x in tempResults]
    print('Create File Results: {}'.format(decodedString))

    tempResults = conn.execute(gpgCommand)
    decodedString = [x.decode('utf-8') for x in tempResults]
    print('GPG Results: {}'.format(decodedString))


createFile = PythonOperator(
    task_id = 'CreateFile',
    provide_context = True,
    python_callable = createTestFile,
    dag = dag
)