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

""" Schedules a set of Drift Detector jobs. """


import argparse
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


def create_drift_detector_task(
    project_id: Text,
    region: Text,
    task_queue: Text,
    service_account: Text,
    template_path: Text,
    schedule_time: datetime.datetime,
    request_response_log_table: Text,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    instance_type: Text,
    feature_names: List[Text],
    output_path: Text,
    schema_file: Text,
    setup_file: Optional[Text] = _SCHEMA_FILE_PATH
) -> Dict:
    """Creates a Cloud Task that submits a run of the Drift Detector template."""

    service_uri = 'https://dataflow.googleapis.com/v1b3/projects/{}/locations/{}/flexTemplates:launch'.format(
        project_id, region)
    job_name = '{}-{}'.format(_JOB_NAME_PREFIX, time.strftime("%Y%m%d-%H%M%S"))

    start_time = start_time.isoformat(sep='T', timespec='seconds')
    end_time = end_time.isoformat(sep='T', timespec='seconds')

    if instance_type == 'OBJECT':
        feature_names = None 
    elif instance_type == 'LIST':
        if not feature_names:
            raise TypeError("Feature names must be provided for the LIST instance types")
        feature_names = ','.join(feature_names)
    else:
        raise TypeError("The instance_type parameter must be LIST or OBJECT")

    parameters = {
        'request_response_log_table': request_response_log_table,
        'instance_type': instance_type,
        'start_time': start_time,
        'end_time': end_time,
        'output_path': output_path,
        'schema_file': schema_file,
        'setup_file': setup_file
    }

    if feature_names:
        parameters['feature_names'] = feature_names

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
        task_queue: str,
        service_account: str,
        region: str,
        template_path: Text,
        beginning_time: datetime.datetime,
        time_window: int,
        num_of_runs: int,
        request_response_log_table: Text,
        instance_type: Text,
        feature_names: List[Text],
        output_root_folder: Text,
        schema_file: Text, 
):
    beginning_time = datetime.datetime(
        beginning_time.year, 
        beginning_time.month, 
        beginning_time.day, 
        beginning_time.hour, 
        beginning_time.minute)

    for run_num in range(num_of_runs):
        end_time = beginning_time + datetime.timedelta(minutes=(run_num + 1)*time_window)
        start_time = end_time - datetime.timedelta(minutes=time_window)
        schedule_time = end_time + datetime.timedelta(minutes=2)
        
        output_path = '{}/{}-{}'.format(
            output_root_folder,
            start_time.isoformat(sep='T', timespec='minutes'),
            end_time.isoformat(sep='T', timespec='minutes'),
        )


        #output_path = 'gs://mlops-dev-workspace/drift_monitor/output/tf/{}'.format(time.strftime("%Y%m%d-%H%M%S"))
        #schema_file = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt'

        print(output_path)
        return
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
            instance_type=instance_type,
            feature_names=feature_names,
            output_path=output_path,
            schema_file=schema_file
        )
        
        print(response)

        logging.log(logging.INFO, response)


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--project',
        help='Project of the queue to add the task to.',
        required=True,
    )

    parser.add_argument(
        '--queue',
        help='ID (short name) of the queue to add the task to.',
        required=True,
    )

    parser.add_argument(
        '--service_account',
        help='An email of a service account to use for submitting calls.',
        required=True,
    )

    parser.add_argument(
        '--location',
        help='Location of the queue to add the task to.',
        required=True,
    )

    parser.add_argument(
        '--model_name',
        help='The AI Platform Prediction model',
        required=True,
    )

    parser.add_argument(
        '--model_version',
        help='The AI Platform Prediction model version',
        required=True
    )

    parser.add_argument(
        '--data_file',
        help="A path to a file with instances.",
        required=True
    )

    parser.add_argument(
        '--start_time',
        help='The start date and time for a simulation in ISO format',
        required=True
    )

    parser.add_argument(
        '--instances_per_call',
        help='The number of instances batched in each call',
        type=int,
        default=3,
    )

    parser.add_argument(
        '--time_between_calls',
        help='The delay between calls in seconds',
        type=int,
        default=60,
    )
    args = parser.parse_args()

    generate_predict_tasks(
        project=args.project,
        queue=args.queue,
        service_account=args.service_account,
        location=args.location,
        model_name=args.model_name,
        model_version=args.model_version,
        data_file=args.data_file,
        start_time=args.start_time,
        instances_per_call=args.instances_per_call,
        time_between_calls=args.time_between_calls)
