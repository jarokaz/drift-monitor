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
import logging
import json
import mock
import pytest
import time

import tensorflow as tf

from scheduler.handlers import run_detector
from scheduler.handlers import schedule_detector


def test_run_detector():
    project_id = 'mlops-dev-env'  
    template_path = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json' 
    region = 'us-central1'
    log_table = 'mlops-dev-env.data_validation.covertype_logs_tf'
    start_time = datetime.datetime.fromisoformat('2020-05-25T16:00:00')
    end_time = datetime.datetime.fromisoformat('2020-05-25T17:00:00')
    output_location = 'gs://mlops-dev-workspace/drift_monitor/output/tf/tests'
    schema_location ='gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt' 
    baseline_stats_location = None

    response = run_detector(
        project_id=project_id,
        region=region,
        template_path=template_path,
        log_table=log_table,
        start_time=start_time,
        end_time=end_time,
        output_location=output_location,
        schema_location=schema_location,
        baseline_stats_location=baseline_stats_location
    ) 
    
    print(type(response))
    print(response)
    

def test_create_drift_detector_task():

    service_account = 'drift-monitor@mlops-dev-env.iam.gserviceaccount.com'
    task_queue = 'drift-monitor-runs'
    schedule_time = datetime.datetime.now() + datetime.timedelta(seconds=30)

    project_id = 'mlops-dev-env'  
    template_path = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json' 
    region = 'us-central1'
    log_table = 'mlops-dev-env.data_validation.covertype_logs_tf'
    start_time = datetime.datetime.fromisoformat('2020-05-25T16:00:00')
    end_time = datetime.datetime.fromisoformat('2020-05-25T17:00:00')
    output_location = 'gs://mlops-dev-workspace/drift_monitor/output/tf/tests'
    schema_location ='gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt' 
    baseline_stats_location = None

    response = schedule_detector(
        task_queue=task_queue,
        service_account=service_account,
        schedule_tiem=schedule_time,
        project_id=project_id,
        region=region,
        template_path=template_path,
        log_table=log_table,
        start_time=start_time,
        end_time=end_time,
        output_location=output_location,
        schema_location=schema_location,
        baseline_stats_location=baseline_stats_location
    ) 
    
    print(type(response))
    print(response)
    print(response)

def test_schedule_drift_detector_runs( ):

    logging.getLogger().setLevel(logging.INFO)

    project_id = 'mlops-dev-env' 
    service_account = 'drift-monitor@mlops-dev-env.iam.gserviceaccount.com'
    template_path = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json'
    region = 'us-central1'
    task_queue = 'drift-monitor-runs'
    request_response_log_table = 'mlops-dev-env.data_validation.covertype_logs_tf'

    beginning_time = datetime.datetime.fromisoformat('2020-05-25T16:00:00')
    time_window = 60 
    num_of_runs = 2
    output_root_folder = 'gs://mlops-dev-workspace/drift_monitor/output/tf'
    schema_file = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt'
    baseline_stats_file = None

    schedule_drift_detector_runs(
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