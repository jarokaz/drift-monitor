#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import base64
import datetime
import json
import mock
import pytest
import time

import tensorflow as tf

from drift_monitor.drift_monitor_scheduler import create_drift_detector_task
from drift_monitor.drift_monitor_scheduler import schedule_drift_detector_runs



def test_create_drift_detector_task():

    project_id = 'mlops-dev-env' 
    service_account = 'drift-monitor@mlops-dev-env.iam.gserviceaccount.com'
    template_path = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json'
    region = 'us-central1'
    task_queue = 'drift-monitor-runs'
    request_response_log_table = 'mlops-dev-env.data_validation.covertype_logs_tf'

    schedule_time = datetime.datetime.now() + datetime.timedelta(seconds=30)
    start_time = datetime.datetime.fromisoformat('2020-05-15T00:15:00')
    end_time = datetime.datetime.fromisoformat('2020-05-15T05:51:00')
    output_path = 'gs://mlops-dev-workspace/drift_monitor/output/tf/{}'.format(time.strftime("%Y%m%d-%H%M%S"))
    schema_file = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt'
    baseline_stats_file = None

    response = create_drift_detector_task(
        project_id=project_id,
        region=region,
        task_queue=task_queue,
        service_account=service_account,
        template_path=template_path,
        schedule_time=schedule_time,
        request_response_log_table=request_response_log_table,
        start_time=start_time,
        end_time=end_time,
        output_path=output_path,
        schema_file=schema_file,
        baseline_stats_file=baseline_stats_file
    )

    print(response)

def test_schedule_drift_detector_runs( ):

    project_id = 'mlops-dev-env' 
    service_account = 'drift-monitor@mlops-dev-env.iam.gserviceaccount.com'
    template_path = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json'
    region = 'us-central1'
    task_queue = 'drift-monitor-runs'
    request_response_log_table = 'mlops-dev-env.data_validation.covertype_classifier_logs_tf'

    #beginning_time = datetime.datetime.now() - datetime.timedelta(minutes=180)
    beginning_time = datetime.datetime.now() - datetime.timedelta(minutes=600) 
    time_window = 60 
    num_of_runs = 24 
    output_root_folder = 'gs://mlops-dev-workspace/drift_monitor/output/tf'
    schema_file = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt'
    baseline_stats_file = None

    response = schedule_drift_detector_runs(
        project_id=project_id,
        task_queue=task_queue,
        service_account=service_account,
        region=region,
        template_path=template_path,
        beginning_time=beginning_time,
        time_window=time_window,
        num_of_runs=num_of_runs,
        request_response_log_table=request_response_log_table,
        output_root_folder=output_root_folder,
        schema_file=schema_file,
        baseline_stats_file=baseline_stats_file
    )