import os
import re

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
        self.hook = SFTPHook(sftp_conn_id)

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)
        fileList = []

        try:
            isFound = False
            directory = self.hook.list_directory(full_path)
            for files in directory:
                if not re.match(file_pattern, files):
                    self.log.info(files)
                    self.log.info(file_pattern)
                else:
                    fileList.append(files)
                    print('I found the file! {}'.format(files))
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