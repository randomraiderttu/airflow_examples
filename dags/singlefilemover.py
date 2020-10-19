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

from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook

LOCAL_LANDING_PATH = '/usr/local/airflow/transfer_dir/'
GPG_HOME = '/usr/local/airflow/.gnupg'


#######################  UTILITY FUNCTIONS #########################################################################
def getMD5sumRemote(sftp_connection, filename):
    """Take connection and full filename location and return back the md5sum
    
    Arguments:
        sftp_connection {pysftp.connection} -- We don't want to open a new connection. Get it from the caller.
        filename {str} -- full filepath of the file we want to get an md5sum for.
    """
    command = "md5sum {}".format(filename)

    tempResults = sftp_connection.execute(command)
    decodedString = [x.decode('utf-8') for x in tempResults]

    # get the first element, strip trailing newline if applicable (and it will be if it's a good md5sum)
    md5sum = decodedString[0].split()[0].rstrip()

    # Evaluate results - an error won't necessarily pass back as an exception, so we need to evaluate the result string
    if md5sum[:6] == 'md5sum':
        raise Exception('Md5sum Error: {}'.format(md5sum)) 
    
    return md5sum

def getMD5sumLocal(fileName):
    """Perform an MD5sum on a local file and return just the md5sum value.
    
    Arguments:
        filename {str} -- full filepath of the file we want to get an md5sum for.
    """
    md5_hash = hashlib.md5()
    with open(fileName,"rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            md5_hash.update(byte_block)
        
    return md5_hash.hexdigest()
#######################  END UTILITY FUNCTIONS #####################################################################


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
    dag_id = 'SingleFileTransferJob',
    description = 'SingleFileTransferJob',
    default_args = args,
    max_active_runs = 100,
    concurrency = 11,
    catchup = False,
    schedule_interval = None,
)

##############################################################################################

def moveFromSourceToLocal(**kwargs):
    """
    Use information from the dag_run passed in by the filefinder DAG to start pulling down a ready file.
    """
    # Variablelize (my word) the dag_run config needed for this step.
    # This might be a good candidate for externalizing
    sftpConn = kwargs['dag_run'].conf['SFTP_Connection_Name']
    sourceFullPath = kwargs['dag_run'].conf['File_Name']
    
    # Strip the ".ready" from the filename as we get the basename of the file
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.ready', '')
    destFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)

    sftpHook = SFTPHook(ftp_conn_id = sftpConn)

    conn = sftpHook.get_conn()

    initialMD5sum = getMD5sumRemote(conn, sourceFullPath)
    logging.info('Initial MD5Sum: {}'.format(initialMD5sum))


    sftpHook.retrieve_file(sourceFullPath, destFullPath)

    currentMD5sum = getMD5sumLocal(destFullPath)
    logging.info('currentMD5Sum: {}'.format(currentMD5sum))
    
    if initialMD5sum != currentMD5sum:
        logging.error('MD5Sum mismatch.  Initial: {}  Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))
        raise Exception('MD5Sum values before and after transfer do not match. Possible transfer issue. Initial: {} Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))

##############################################################################################

def decryptFile(**kwargs):
    """
    Decrypts the file on the transfer directory in preparation for sending to final destination
    """
    # Strip the ".ready" from the filename as we get the basename of the file
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.ready', '')
    fileFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)
    pgpPassPhrase = kwargs['dag_run'].conf['PGP_Passphrase_Variable']

    gpg = gnupg.GPG(gnupghome = GPG_HOME, verbose = True)

    curFile = os.path.splitext(fileFullPath)[0]

    try:
        with open(fileFullPath, 'rb') as f:
            status = gpg.decrypt_file(f, passphrase = 'airflow101', output = curFile)
            logging.info('the type of this status field is: {}'.format(type(status)))

            if not status.ok:
                logging.error('Decryption failed. Status {}'.format(status.status))
                raise Exception('Decryption failed.')                

            logging.info('Decryption of {} successful'.format(fileFullPath))
    except Exception as e:
        logging.error('Error while decrypting {}. Unable to proceed.'.format(fileFullPath))
        raise e

##############################################################################################

def createTriggerFile(**kwargs):
    """
    Create trigger file for decrypted file
    """
    # This is the full path on the remote machine, need to get to base filename without gpg ready
    localDecryptedFullFilePath = os.path.join(LOCAL_LANDING_PATH, os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.gpg.ready', ''))
    triggerFileName = localDecryptedFullFilePath + '.trigger'

    md5sum = getMD5sumLocal(localDecryptedFullFilePath)

    # write the timestamp in trigger file as central time
    timezone = pytz.timezone('US/Central')
    curDate = datetime.now(timezone)

    with open (triggerFileName, 'w') as f:
        f.write(curDate.strftime("%m/%d/%Y %H:%M:%S") + '\n')
        f.write(md5sum + '\n')


##############################################################################################

def moveFileFromLocalToDest(**kwargs):
    """
    Take decrypted file and move to destination location
    """
    """
    Use information from the dag_run passed in by the filefinder DAG to start pulling down a ready file.
    """
    # Variablelize (my word) the dag_run config needed for this step.
    # This might be a good candidate for externalizing
    sftpConn = kwargs['dag_run'].conf['Feed_Dest_Connection_Name']
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.gpg.ready', '')
    destPath = kwargs['dag_run'].conf['Feed_Dest_Location']

    sourceFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)
    destFullPath = os.path.join(destPath, fileName)

    logging.info('Attempting to transfer {} on airflow host to {}@{} site'.format(sourceFullPath, destFullPath, sftpConn))


    sftpHook = SFTPHook(ftp_conn_id = sftpConn)

    conn = sftpHook.get_conn()

    initialMD5sum = getMD5sumLocal(sourceFullPath)
    logging.info('Local MD5Sum: {}'.format(initialMD5sum))

    sftpHook.store_file(destFullPath, sourceFullPath)

    currentMD5sum = getMD5sumRemote(conn, destFullPath)
    logging.info('Remote MD5Sum: {}'.format(currentMD5sum))
    
    if initialMD5sum != currentMD5sum:
        logging.error('MD5Sum mismatch.  Initial: {}  Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))
        raise Exception('MD5Sum values before and after transfer do not match. Possible transfer issue. Initial: {} Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))

    logging.info('Trasfer Succeeded.')

##############################################################################################

def moveTriggerFromLocalToDest(**kwargs):
    """
    Take trigger file and move to destination location.
    Trigger file is always secondary.  You want it to land on the destination after a successful transfer of the data file.
    """
    # Variablelize (my word) the dag_run config needed for this step.
    # This might be a good candidate for externalizing
    sftpConn = kwargs['dag_run'].conf['Feed_Dest_Connection_Name']
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.gpg.ready', '') + '.trigger'
    destPath = kwargs['dag_run'].conf['Feed_Dest_Location']

    sourceFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)
    destFullPath = os.path.join(destPath, fileName)

    logging.info('Attempting to transfer {} on airflow host to {}@{} site'.format(sourceFullPath, destFullPath, sftpConn))


    sftpHook = SFTPHook(ftp_conn_id = sftpConn)

    conn = sftpHook.get_conn()

    initialMD5sum = getMD5sumLocal(sourceFullPath)
    logging.info('Local MD5Sum: {}'.format(initialMD5sum))

    sftpHook.store_file(destFullPath, sourceFullPath)

    currentMD5sum = getMD5sumRemote(conn, destFullPath)
    logging.info('Remote MD5Sum: {}'.format(currentMD5sum))
    
    if initialMD5sum != currentMD5sum:
        logging.error('MD5Sum mismatch.  Initial: {}  Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))
        raise Exception('MD5Sum values before and after transfer do not match. Possible transfer issue. Initial: {} Post-Transfer: {}'.format(initialMD5sum, currentMD5sum))

    logging.info('Trasfer Succeeded.')

##############################################################################################

def moveToArchive(**kwargs):
    """
    Move original gpg file to archive
    """
    archiveDirectory = os.path.join(LOCAL_LANDING_PATH, 'archive')
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.ready', '')
    sourceFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)
    destFullPath = os.path.join(archiveDirectory, fileName)

    os.replace(sourceFullPath, destFullPath)
    logging.info('{} transferred to archive directory.'.format(fileName))

##############################################################################################

def removeFilesFromLocalDir(**kwargs):
    """
    Move original gpg file to archive
    """
    fileName = os.path.basename(kwargs['dag_run'].conf['File_Name']).replace('.gpg.ready', '')
    triggerFileName = fileName + '.trigger'
    fileFullPath = os.path.join(LOCAL_LANDING_PATH, fileName)
    triggerFullPath = os.path.join(LOCAL_LANDING_PATH, triggerFileName)

    if os.path.exists(fileFullPath):
        os.remove(fileFullPath)

    if os.path.exists(triggerFullPath):
        os.remove(triggerFullPath)

    logging.info('{} and {} removed from local transfer directory.'.format(fileName, triggerFileName))

##############################################################################################

def removeFileFromSFTP(**kwargs):
    """
    Delete file from SFTP
    """
    sftpConn = kwargs['dag_run'].conf['SFTP_Connection_Name']
    fileName = kwargs['dag_run'].conf['File_Name']

    sftpHook = SFTPHook(ftp_conn_id = sftpConn)

    logging.info('Attempting to delete {} from {}'.format(fileName, sftpConn))
    sftpHook.delete_file(fileName)
    logging.info('Deletion Successful')

##############################################################################################

sourceToTemp = PythonOperator(
    task_id = 'MoveFromSourceToLocal',
    provide_context = True,
    python_callable = moveFromSourceToLocal,
    dag = dag
)

decryptFile = PythonOperator(
    task_id = 'DecryptFile',
    provide_context = True,
    python_callable = decryptFile,
    dag = dag
)

createTriggerFile = PythonOperator(
    task_id = 'CreateTriggerFile',
    provide_context = True,
    python_callable = createTriggerFile,
    dag = dag
)

moveDataFileToDest = PythonOperator(
    task_id = 'MoveDataFileToDest',
    provide_context = True,
    python_callable = moveFileFromLocalToDest,
    dag = dag
)

moveTriggerFileToDest = PythonOperator(
    task_id = 'MoveTriggerFileToDest',
    provide_context = True,
    python_callable = moveTriggerFromLocalToDest,
    dag = dag
)

moveToArchive = PythonOperator(
    task_id = 'MoveOrigFileToArchive',
    provide_context = True,
    python_callable = moveToArchive,
    dag = dag
)

removeTempFiles = PythonOperator(
    task_id = 'RemoveTempFilesFromLocalDir',
    provide_context = True,
    python_callable = removeFilesFromLocalDir,
    dag = dag
)

removeFileFromSFTP = PythonOperator(
    task_id = 'RemoveFileFromSFTP',
    provide_context = True,
    python_callable = removeFileFromSFTP,
    dag = dag
)

sourceToTemp >> decryptFile >> createTriggerFile >> moveDataFileToDest >> moveTriggerFileToDest >> [moveToArchive, removeTempFiles] >> removeFileFromSFTP