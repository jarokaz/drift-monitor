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

from typing import List, Optional, Text, Union, Dict
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

def create_predict_task(project: str,
                     queue: str,
                     location: str,
                     model_name: str,
                     model_version: str,
                     instances: List[Union[Dict,List]],
                     service_account: str,
                     execute_time: datetime):
                     
    """Creates a task that calls AI Platform Prediction service.
    
    Args:
        queue: A Cloud Task queue to use.
        location: GCP region.
        model_name: AI Platform Prediction Model name.
        model_version: AI Platform Prediction Model version.
        instances: A list of instances to inference on.
        service_account: A service account to use to execute the call.
        execute_time: A time when to execute the call.
    
    """

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, location, queue)

    service_uri = 'https://ml.googleapis.com/v1/projects/{}/models/{}/versions/{}:predict'.format(
    project, model_name, model_version)
    instances = {'instances': instances}

    task = {
            'http_request': {  
                'http_method': 'POST',
                'url': service_uri,
                'body': json.dumps(instances).encode(),
                'headers': {'content-type': 'application/json'},
                'oauth_token': {'service_account_email': service_account}
            }
    }
    
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(execute_time)
    task['schedule_time'] = timestamp

    response = client.create_task(parent, task)
    return response


#if __name__ == '__main__':
#    parser = argparse.ArgumentParser(
#        description=create_http_task.__doc__,
#        formatter_class=argparse.RawDescriptionHelpFormatter)
#
#    parser.add_argument(
#        '--project',
#        help='Project of the queue to add the task to.',
#        required=True,
#    )
#
#    parser.add_argument(
#        '--queue',
#        help='ID (short name) of the queue to add the task to.',
#        required=True,
#    )
#
#    parser.add_argument(
#        '--location',
#        help='Location of the queue to add the task to.',
#        required=True,
#    )
#
#    parser.add_argument(
#        '--url',
#        help='The full url path that the request will be sent to.',
#        required=True,
#    )
#
#    parser.add_argument(
#        '--payload',
#        help='Optional payload to attach to the push queue.'
#    )
#
#    parser.add_argument(
#        '--in_seconds', type=int,
#        help='The number of seconds from now to schedule task attempt.'
#    )
#
#    parser.add_argument(
#        '--task_name',
#        help='Task name of the task to create'
#    )
#    args = parser.parse_args()
#
#    create_http_task(
#        args.project, args.queue, args.location, args.url,
#        args.payload, args.in_seconds, args.task_name)