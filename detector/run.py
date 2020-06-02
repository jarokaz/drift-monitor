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

"""Runs a data drift job."""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import datetime
import logging
import os
import re
import apache_beam as beam

from typing import List, Optional, Text, Union, Dict, Iterable
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

from tensorflow_data_validation import StatsOptions
from tensorflow_data_validation import load_statistics
from tensorflow_data_validation import load_schema_text

from utils.drift_reports import generate_drift_reports

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--request_response_log_table',
        dest='request_response_log_table',
        type=str,
        required=True,
        help='A full name of the AI Platform Prediction request-response log table')
    parser.add_argument(
        '--model',
        dest='model',
        type=str,
        required=True,
        help='A name of the AI Platform Prediction model')
    parser.add_argument(
        '--version',
        dest='version',
        type=str,
        required=True,
        help='A name of the AI Platform Prediction model version')
    parser.add_argument(
        '--start_time',
        dest='start_time',
        type=str,
        required=True,
        help='The beginning of a time series of log records in the ISO datetime format')
    parser.add_argument(
        '--end_time',
        dest='end_time',
        type=str,
        required=True,
        help='The end of a time series of log records in the ISO datetime format')
    parser.add_argument(
        '--output_path',
        dest='output_path',
        type=str,
        required=True,
        help='An output path for statistics and anomaly protocol buffers')
    parser.add_argument(
        '--schema_file',
        dest='schema_file',
        type=str,
        help='A path to a schema file',
        required=True)
    parser.add_argument(
        '--baseline_stats_file',
        dest='baseline_stats_file',
        type=str,
        help='A path to a baseline statistics file',
        required=False)
    parser.add_argument(
        '--time_window',
        dest='time_window',
        type=str,
        help='A time window to use for time slice calculations. You must use the m or h suffix to designate minutes or hours',
        required=False)

    known_args, pipeline_args = parser.parse_known_args()
    if known_args.baseline_stats_file:
        baseline_stats = load_statistics(known_args.baseline_stats_file)
    else:
        baseline_stats = None

    schema = load_schema_text(known_args.schema_file)

    start_time = datetime.datetime.fromisoformat(known_args.start_time)
    end_time = datetime.datetime.fromisoformat(known_args.end_time) 

    if start_time >= end_time:
        raise ValueError("The end_time cannot be earlier than start_time")

    time_window=None
    if known_args.time_window:
        if not re.fullmatch('[0-9]+[hm]', known_args.time_window):
            raise ValueError("Incorrect format of time_window")
        if known_args.time_window[-1]=='h': 
            time_window = datetime.timedelta(hours=int(known_args.time_window[0:-1]))
        else:
            time_window = datetime.timedelta(minutes=int(known_args.time_window[0:-1]))

    pipeline_options = PipelineOptions(pipeline_args)

    _ = generate_drift_reports(
        request_response_log_table=known_args.request_response_log_table,
        model=known_args.model,
        version=known_args.version,
        start_time=start_time,
        end_time=end_time,
        output_path=known_args.output_path,
        schema=schema,
        baseline_stats=baseline_stats,
        time_window=time_window,
        pipeline_options=pipeline_options)
