import os
import re
import time

import logging
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.sensors.base_sensor_operator import BaseSensorOperator


class SFTPSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, sftp_conn_id='sftp_default', *args, **kwargs):
        super(SFTPSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern
        self.sftp_conn_id = sftp_conn_id
        self.hook = SFTPHook(ftp_conn_id = sftp_conn_id, keepalive_interval = 10)

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)
        fileDict = {}
        fileList = []

        try:
            isFound = False
            directory = self.hook.describe_directory(full_path)
            logging.info('Polling Interval 1')
            for file in directory.keys():
                if not re.match(file_pattern, file):
                    self.log.info(file)
                    self.log.info(file_pattern)
                    del directory[file]

            if not directory:
                # If directory has no files that match the mask, exit
                return isFound

            # wait before we compare file sizes and timestamps again to 
            # verify that the file is done transferring to remote loc
            time.sleep(30)

            logging.info('Post-Wait Polling')
            newDirectoryResults = self.hook.describe_directory(full_path)

            for file in newDirectoryResults.keys():
                if file in directory.keys():
                    if newDirectoryResults[file]['size'] == directory[file]['size'] and \
                        newDirectoryResults[file]['modify'] == directory[file]['modify']:

                        fileList.append(file)
                        print('filename: {} with size {} and modified time of {} met all criteria to be moved.'.format
                            (file, newDirectoryResults[file]['size'],newDirectoryResults[file]['modify']))
                        isFound = True


            context["task_instance"].xcom_push("file_name", fileList)

            return isFound
        except IOError as e:
            if e.errno != SFTP_NO_SUCH_FILE:
                raise e
            return False

class SFTPSensorPlugin(AirflowPlugin):
    name = "sftp_sensor"
    sensors = [SFTPSensor]