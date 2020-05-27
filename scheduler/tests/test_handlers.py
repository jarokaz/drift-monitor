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

from scheduler.handlers import run_drift_detector
from scheduler.handlers import schedule_drift_detector
from scheduler.handlers import _prepare_drift_detector_request_body

DEFAULT_TEMPLATE_PATH = 'gs://mlops-dev-workspace/flex-templates/drift-detector.json'  
DEFAULT_PROJECT_ID = 'mlops-dev-env'
DEFAULT_LOG_TABLE = 'mlops-dev-env.data_validation.covertype_logs_tf'
DEFAULT_START_TIME = '2020-05-25T16:00:00'
DEFAULT_END_TIME = '2020-05-25T17:00:00' 
DEFAULT_OUTPUT_LOCATION ='gs://mlops-dev-workspace/drift_monitor/output/tf/tests' 
DEFAULT_SCHEMA_LOCATION = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt' 
DEFAULT_SERVICE_ACCOUNT = 'drift-monitor@mlops-dev-env.iam.gserviceaccount.com'
DEFAULT_REGION = 'us-central1'
DEFAULT_TASK_QUEUE = 'drift-monitor-runs'


def test_prepare_drift_detector_request_body():
    
    job_name = '{}-{}'.format('data-drift-detector', time.strftime("%Y%m%d-%H%M%S"))
    template_path = DEFAULT_TEMPLATE_PATH
    output_location = '{}/{}'.format(DEFAULT_OUTPUT_LOCATION, 'testing_body')
    log_table = DEFAULT_LOG_TABLE
    start_time = DEFAULT_START_TIME
    end_time = DEFAULT_END_TIME
    schema_location = DEFAULT_SCHEMA_LOCATION 
    baseline_stats_location = None

    body = _prepare_drift_detector_request_body(
        job_name=job_name,
        template_path=template_path,
        log_table=log_table,
        start_time=start_time,
        end_time=end_time,
        output_location=output_location,
        schema_location=schema_location,
        baseline_stats_location=baseline_stats_location
    )

    print(body)


def test_run_drift_detector():
    project_id = DEFAULT_PROJECT_ID 
    template_path = DEFAULT_TEMPLATE_PATH
    region = DEFAULT_REGION 
    log_table = DEFAULT_LOG_TABLE
    start_time = datetime.datetime.fromisoformat(DEFAULT_START_TIME)
    end_time = datetime.datetime.fromisoformat(DEFAULT_END_TIME)
    output_location = DEFAULT_OUTPUT_LOCATION 
    schema_location = DEFAULT_SCHEMA_LOCATION 
    baseline_stats_location = None

    response = run_drift_detector(
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
    
    print(response)
    

def test_schedule_drift_detector():

    service_account = DEFAULT_SERVICE_ACCOUNT 
    task_queue = DEFAULT_TASK_QUEUE 
    schedule_time = datetime.datetime.now() + datetime.timedelta(seconds=30)

    project_id = DEFAULT_PROJECT_ID  
    template_path = DEFAULT_TEMPLATE_PATH
    region = DEFAULT_REGION 
    log_table = DEFAULT_LOG_TABLE
    start_time = datetime.datetime.fromisoformat(DEFAULT_START_TIME)
    end_time = datetime.datetime.fromisoformat(DEFAULT_END_TIME)
    output_location = DEFAULT_OUTPUT_LOCATION 
    schema_location =DEFAULT_SCHEMA_LOCATION 
    baseline_stats_location = None

    response = schedule_drift_detector(
        task_queue=task_queue,
        service_account=service_account,
        schedule_time=schedule_time,
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
    
    print(response)

