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

import tensorflow as tf

from utils.drift_reports import generate_drift_reports, InstanceType
from tensorflow_data_validation import load_schema_text
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


def test_generate_drift_reports():

    request_response_log_table = 'data_validation.covertype_classifier_logs_tf'
    project_id = 'mlops-dev-env'
    baseline_stats = None
    output_path = 'gs: // mlops-dev-workspace/drift_monitor/output/tf'
    start_time = '2020-05-15T00:15:00'
    end_time = '2020-05-15T05:51:00'

    schema_path = 'gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt'
    schema = load_schema_text(schema_path)

    instance_type = InstanceType.JSON_OBJECT

    feature_names = None

    pipeline_options = PipelineOptions() 
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = project_id

    generate_drift_reports(
        request_response_log_table=request_response_log_table,
        instance_type=instance_type,
        feature_names=feature_names,
        start_time=start_time,
        end_time=end_time,
        output_path=output_path,
        schema=schema, 
        baseline_stats=baseline_stats,
        pipeline_options=pipeline_options)