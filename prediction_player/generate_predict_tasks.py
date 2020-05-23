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

""" Creates a Cloud Task that invokes an AI Platform Prediction version. """


import argparse
import datetime
import json
import googleapiclient.discovery
import logging

from typing import List, Optional, Text, Union, Dict
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

def create_predict_task(
    project: str,
    queue: str,
    service_account: str,
    location: str,
    model_name: str,
    model_version: str, 
    instances: List[Union[Dict,List]],
    execute_time: datetime):
                        
    """Creates a task that calls AI Platform Prediction service."""
    
    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    service_uri = 'https://ml.googleapis.com/v1/projects/{}/models/{}/versions/{}:predict'.format(
    project, model_name, model_version)
    instances = json.dumps({'instances': instances})

    task = {
            'http_request': {  
                'http_method': 'POST',
                'url': service_uri,
                'body': instances.encode(),
                'headers': {'content-type': 'application/json'},
                'oauth_token': {'service_account_email': service_account}
            }
    }

    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(execute_time)
    task['schedule_time'] = timestamp

    response = client.create_task(parent, task)
    logging.info("Created task: {}".format(response.name))

    return response


def generate_predict_tasks(
    project: str,
    queue: str,
    service_account: str,
    location: str,
    model_name: str,
    model_version: str,
    data_file: str,
    start_time: datetime,
    instances_per_call: int,
    time_between_calls: int):

    """Creates a set of tasks that call AI Platform Prediction service."""
    
    #with open(data_file, 'r') as json_examples:
    #    instances = []
    #    execute_time = datetime.datetime.fromisoformat(start_time)
    #    for json_example in json_examples:
    #        instances.append(json.loads(json_example))
    #        if len(instances) == instances_per_call:
    #            create_predict_task(
    #                   project: str,
    #                   queue: str,
    #                   service_account: str,
    #                   location: str,
    #                   model_name: str,
    #                   model_version: str, instances, 
    #             execute_time + datetime.timedelta(seconds=time_between_calls)
    #            instances = []
    #    if len(instances):
    #        _create_predict_task(instances, execute_time)

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