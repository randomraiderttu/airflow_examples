from datetime import datetime, timedelta
import time
import logging
import os
import pysftp
import re
import json

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.operators.python_operator import PythonOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag # Needed to trigger dags
from airflow.utils import timezone # Needed to determine utc in iso format for passing the run_id to the triggered dag.

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


# Constants
SLEEP_TIME = 10

args = {
    'start_date': datetime(2020,2,20,0,0,0),
    'email': [],
    'depends_on_past': False,
    'email_on_failure': False,
    'provide_context': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds = 30),
    'execution_timeout': timedelta(minutes = 2),
}

dag = DAG(
    dag_id = 'SFTP_Pollster',
    description = 'SFTP_Pollster',
    default_args = args,
    max_active_runs = 1,
    concurrency = 30,
    catchup = False,
    schedule_interval = '*/2 * * * *',
)

dag_config = Variable.get('SFTP_Polling_JSON', deserialize_json = True)


def pollForFiles(**kwargs):
    # Create some local scope variables for use later in proc
    sftpName = kwargs['SFTP_Name']
    sftpConnName = kwargs['SFTP_Connection_Name']
    feedGroups = kwargs['Feed_Groups']
    
    # Connect to SFTP site using provided credentials - should be saved in Connections
    sourceHook = SFTPHook(ftp_conn_id = sftpConnName)

    # Create empty dictionary for storing files that match file masks
    fileMatches = {}

    # Loop through feed locations and their regex for this SFTP site.
    for i in feedGroups:
        fullPath = i['Feed_Group_Location']
        filePattern = i['Feed_Group_Regex']
        feedGroupName = i['Feed_Group_Name']

        try:
            directory = sourceHook.describe_directory(path = fullPath)
            for file in directory.keys():
                if re.match(filePattern, file):
                    fileMatches[os.path.join(fullPath, file)] = directory[file]
        except Exception as e:
            logging.error('Error attempting to poll feed group {} in directory {}'.format(feedGroupName, fullPath))
            raise e

    # If we do not find a file that matches a file mask in any of the directories, exit.
    if not fileMatches:
        return 0

    # If no trigger files or renaming is utilized by the client when placing files on SFTP, we
    #   have to resort to polling for files, waiting for a time period and then comparing the size/modified time
    #   to see if they are ready to pull down.
    time.sleep(SLEEP_TIME)

    for j in feedGroups:
        fullPath = j['Feed_Group_Location']
        filePattern = j['Feed_Group_Regex']
        feedGroupName = j['Feed_Group_Name']
        newFileMatches = {}

        try:
            newDirResults = sourceHook.describe_directory(fullPath)
            # Add only the files that match regular expression for this feed group
            for file in newDirResults.keys():
                if re.match(filePattern, file):
                    newFileMatches[os.path.join(fullPath, file)] = newDirResults[file]

            for file in newFileMatches.keys():
                # fullFilePath = os.path.join(fullPath, file)

                if file in fileMatches.keys():
                    if newFileMatches[file]['size'] == fileMatches[file]['size'] and \
                            newFileMatches[file]['modify'] == fileMatches[file]['modify']:
                        
                        readyFile = file + '.ready'
                        
                        # If file hasn't changed size or modified time since first look, set to ready for another process to pick up and transfer.
                        sourceHook.conn.rename(file, readyFile)
                        logging.info('SFTP: {} FeedGroup: {} File: {} is ready.'.format(sftpName, feedGroupName, os.path.basename(file)))
                        
                        triggerConfig = {
                            'SFTP_Name': sftpName, 
                            'SFTP_Connection_Name': sftpConnName,
                            'File_Name': readyFile,
                        }

                        triggerConfig.update(j)
                        
                        trigger_dag(
                            dag_id = 'SingleFileTransferJob',
                            run_id = 'trig_{}'.format(timezone.utcnow().isoformat()),
                            conf = json.dumps(triggerConfig),
                            execution_date = None,
                            replace_microseconds = False
                        )
        except Exception as e:
            logging.error('Error attempting to rename files in feed group {} in directory {}'.format(feedGroupName, fullPath))
            raise e
            

# Dynamically create a task for every SFTP site we need to connect and poll - run in parallel up to max concurrency set by the DAG definition
for i in dag_config['SFTP_Polling_Sites']:
    sftpName = i['SFTP_Name']

    task = PythonOperator(
        task_id = 'Polling_{}_Site'.format(sftpName),
        python_callable = pollForFiles,
        op_kwargs = {'SFTP_Name': sftpName, 'SFTP_Connection_Name': i['SFTP_Connection_Name'], 'Feed_Groups': i['Feed_Groups']},
        dag = dag
    )

