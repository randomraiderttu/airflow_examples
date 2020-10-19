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

import logging

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import datetime, timedelta

##########################################################################################

args = {
    'start_date': datetime(2020,2,20,0,0,0),
    'email': ['bhudson@brierley.com', 'byasam@brierley.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'provide_context': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds = 30),
    'execution_timeout': timedelta(minutes = 2),
}

dag = DAG(
    dag_id = 'DAP_Job_Failure',
    description = 'DAP_Job_Failure',
    default_args = args,
    max_active_runs = 1,
    concurrency = 11,
    catchup = False,
    schedule_interval = None,
)

##########################################################################################

def runDAP(**kwargs):
    """
    Connects to App Server via SSH and executes script, capturing and reporting output.
    """

    sshSource = SSHHook(ssh_conn_id = 'DAP_App_Server')


    command = 'E:\\Airflow_Test\\DAP\DAPConsoleProcessor.exe -config "E:\\Airflow_Test\\DAP\\Configuration\\DAPOrderCancellation.xml" -jobname "DAPOrderCancellation.xml"'

    try:
        sshConn = sshSource.get_conn()
    
        stdIn, stdOut, stdErr = sshConn.exec_command(command = command)

        exitStatus = stdOut.channel.recv_exit_status()
        errorMessage = stdErr.read().decode('ascii')
        stdOutput = stdOut.read().decode('ascii')

        if exitStatus == 0:
            print('DAP Started Successfully.')

        if errorMessage:
            logging.error('DAP Processor Failure. See Exception')
            raise Exception(errorMessage)

    finally:
        print('Exit Status: {}'.format(exitStatus))
        print('StdOut: {}'.format(stdOutput))
        print('StdErr: {}'.format(errorMessage))

        if sshConn:
            sshConn.close()

# run_DAP_Processor = SSHOperator(
#     task_id = 'Run_DAP_Processor',
#     provide_context = True, 
#     ssh_conn_id = 'DAP_App_Server',
#     # command = 'E:\\Airflow_Test\\DAP\DAPConsoleProcessor.exe -config "E:\\Airflow_Test\\DAP\\Configuration\\DAPTCPBrands.xml" -jobname "DAPTCPBrands.xml"',
#     command = 'whoami',
#     timeout = 3600,
#     do_xcom_push = True,
#     get_pty = True,
#     dag = dag
# )


runDAPTask = PythonOperator(
    task_id = 'Run_DAP_Processor',
    provide_context = True,
    python_callable = runDAP,
    dag = dag
)
