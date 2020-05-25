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
import logging
import os

from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner

from tensorflow_data_validation import StatsOptions
from tensorflow_data_validation import load_statistics
from tensorflow_data_validation import load_schema_text

from utils.drift_reports import generate_drift_reports
from utils.drift_reports import InstanceType

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--request_response_log_table',
        dest='request_response_log_table',
        required=True,
        help='Full name of AI Platform Prediction request-response log table')
    parser.add_argument(
        '--start_time',
        dest='start_time',
        required=True,
        help='The beginning of a time window')
    parser.add_argument(
        '--end_time',
        dest='end_time',
        required=True,
        help='The end of a time window')
    parser.add_argument(
        '--output_path',
        dest='output_path',
        required=True,
        help='Output path')
    parser.add_argument(
        '--schema_file',
        dest='schema_file',
        help='A path to a schema file',
        required=True)
    parser.add_argument(
        '--baseline_stats_file',
        dest='baseline_stats_file',
        help='A path to a baseline statistics file',
        required=False)

    known_args, pipeline_args = parser.parse_known_args()
    if known_args.baseline_stats_file:
        baseline_stats = load_statistics(known_args.baseline_stats_file)
    else:
        baseline_stats = None
    schema = load_schema_text(known_args.schema_file)
    pipeline_options = PipelineOptions(pipeline_args)

    _ = generate_drift_reports(
        request_response_log_table=known_args.request_response_log_table,
        start_time=known_args.start_time,
        end_time=known_args.end_time,
        output_path=known_args.output_path,
        schema=schema,
        baseline_stats=baseline_stats,
        pipeline_options=pipeline_options)
