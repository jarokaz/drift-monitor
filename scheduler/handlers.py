# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Helper routines to trigger runs of the drift detector Dataflow template. """


import argparse
import click
import datetime
import time
import json
import googleapiclient.discovery
import logging

from typing import List, Optional, Text, Union, Dict
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2


_SCHEMA_FILE_PATH = './setup.py'
_JOB_NAME_PREFIX = 'data-drift-detector'

def _prepare_drift_detector_request_body(
    job_name: Text,
    template_path: Text,
    log_table: Text,
    start_time: Text ,
    end_time: Text,
    output_location: Text,
    schema_location: Text,
    baseline_stats_location: Text
) -> Dict:
    """Prepares a body of the data drift Dataflow template run request."""

    parameters = {
        'request_response_log_table': log_table,
        'start_time': start_time,
        'end_time': end_time,
        'output_path': output_location,
        'schema_file': schema_location,
        'setup_file': _SCHEMA_FILE_PATH
    }

    if baseline_stats_location:
        parameters['baseline_stats_path'] = baseline_stats_location 
    
    body = {
        'launch_parameter': 
            {
                'jobName': job_name,
                'parameters' : parameters,
                'containerSpecGcsPath': template_path
            }}

    return body
    
def run_detector(
    project_id: Text,
    region: Text,
    template_path: Text,
    log_table: Text,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    output_location: Text,
    schema_location: Text,
    baseline_stats_location: Optional[Text]=None,
) -> Dict:
    """Runs the drift detector Dataflow template."""

    service = googleapiclient.discovery.build('dataflow', 'v1b3')

    job_name = '{}-{}'.format(_JOB_NAME_PREFIX, time.strftime("%Y%m%d-%H%M%S"))
    start_time = start_time.isoformat(sep='T', timespec='seconds')
    end_time = end_time.isoformat(sep='T', timespec='seconds')
    output_location = '{}/{}_{}_{}'.format(output_location, job_name, start_time, end_time)

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

    request = service.projects().locations().flexTemplates().launch(
        location=region,
        projectId=project_id,
        body=body)

    response = request.execute()

    return response


def schedule_detector(
    task_queue: Text,
    service_account: Text,
    schedule_time: datetime.datetime,
    project_id: Text,
    region: Text,
    template_path: Text,
    log_table: Text,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    output_location: Text,
    schema_location: Text,
    baseline_stats_location: Optional[Text]=None,
) -> Dict:
    """Creates a Cloud Task that submits a run of the drift detector template."""

    service_uri = 'https://dataflow.googleapis.com/v1b3/projects/{}/locations/{}/flexTemplates:launch'.format(
        project_id, region)
    job_name = '{}-{}'.format(_JOB_NAME_PREFIX, time.strftime("%Y%m%d-%H%M%S"))

    start_time = start_time.isoformat(sep='T', timespec='seconds')
    end_time = end_time.isoformat(sep='T', timespec='seconds')

    parameters = {
        'request_response_log_table': request_response_log_table,
        'start_time': start_time,
        'end_time': end_time,
        'output_path': output_path,
        'schema_file': schema_file,
        'setup_file': setup_file
    }

    if baseline_stats_file:
        parameters['baseline_stats_file'] = baseline_stats_file

    body = {
        'launch_parameter':
            {
                'jobName': job_name,
                'parameters': parameters,
                'containerSpecGcsPath': template_path
            }}
    
    task = {
        'http_request': {
            'http_method': 'POST',
            'url': service_uri,
            'body': json.dumps(body).encode(),
            'headers': {'content-type': 'application/json'},
            'oauth_token': {'service_account_email': service_account}
        }
    }

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(schedule_time)
    task['schedule_time'] = timestamp

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project_id, region, task_queue)

    response = client.create_task(parent, task)
    logging.info("Created task: {}".format(response.name))

    return response


def schedule_drift_detector_runs(
        project_id: str,
        region: str,
        task_queue: str,
        service_account: str,
        template_path: Text,
        beginning_time: datetime.datetime,
        time_window: int,
        num_of_runs: int,
        request_response_log_table: Text,
        output_root_folder: Text,
        schema_file: Text,
        baseline_stats_file: Optional[Text] = None 
):
    """Schedules a series of drift detector runs."""

    logging.getLogger().setLevel(logging.INFO)

    beginning_time = beginning_time.replace(microsecond=0)
    beginning_time = beginning_time.replace(second=0)

    for run_num in range(num_of_runs):
        end_time = beginning_time + datetime.timedelta(minutes=(run_num + 1)*time_window)
        start_time = end_time - datetime.timedelta(minutes=time_window)
        schedule_time = end_time + datetime.timedelta(minutes=2)

        output_path = '{}/{}_{}'.format(
            output_root_folder,
            start_time.isoformat(sep='T', timespec='minutes'),
            end_time.isoformat(sep='T', timespec='minutes'),
        )

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

        logging.log(logging.INFO, response)



