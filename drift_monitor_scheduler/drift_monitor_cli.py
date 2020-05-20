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
import logging


from drift_monitor.drift_monitor_scheduler import schedule_drift_detector_runs

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
