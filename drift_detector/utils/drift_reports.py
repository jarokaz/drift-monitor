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
"""A utility function to generate data drift reports for a time window
in AI Platform Prediction request-response log.
"""

import datetime
import os
import logging
from enum import Enum
from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery
from jinja2 import Template

from tensorflow_data_validation import GenerateStatistics
from tensorflow_data_validation import validate_statistics
from tensorflow_data_validation import utils

from tensorflow_metadata.proto.v0 import statistics_pb2
from tensorflow_metadata.proto.v0 import schema_pb2

from coders.beam_example_coders import InstanceCoder


_STATS_FILENAME = 'stats.pb'
_ANOMALIES_FILENAME = 'anomalies.pbtxt'

_LOGGING_TABLE_SCHEMA = {
    'model': 'STRING',
    'model_version':  'STRING',
    'time': 'TIMESTAMP',
    'raw_data': 'STRING',
    'raw_prediction': 'STRING',
    'groundtruth': 'STRING'
}


def _validate_request_response_log_schema(request_response_log: str):
    """Validates that a provided request response log table
    conforms to schema."""

    query_template = """
       SELECT *
       FROM 
           `{{ source_table }}` 
       LIMIT 1
       """

    query = Template(query_template).render(
        source_table=request_response_log)

    client = bigquery.Client()
    query_job = client.query(query)
    rows = query_job.result()
    schema = {field.name: field.field_type for field in rows.schema}

    if schema != _LOGGING_TABLE_SCHEMA:
        raise TypeError("The table - {} - does not conform to the reuquest_response log table schema". format(
            request_response_log))


def _generate_query(table_name, start_time, end_time):
    """Prepares a data sampling query."""

    sampling_query_template = """
       SELECT *
       FROM 
           `{{ source_table }}` AS cover
       WHERE time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
       """

    query = Template(sampling_query_template).render(
        source_table=table_name, start_time=start_time, end_time=end_time)

    return query


def generate_drift_reports(
        request_response_log_table: str,
        start_time: str,
        end_time: str,
        output_path: str,
        schema: schema_pb2.Schema,
        baseline_stats: statistics_pb2.DatasetFeatureStatisticsList,
        pipeline_options: Optional[PipelineOptions] = None,
):
    """Computes statistics and anomalies for a time window in AI Platform Prediction
    request-response log.

    Args:
      request_response_log_table: A full name of a BigQuery table
        with the request_response_log
      start_time: The beginning of a time window in the ISO time format.
      end_time: The end of a time window in the ISO time format.
      output_path: The GCS location to output the statistics and anomalies
        proto buffers to. The file names will be `stats.pb` and `anomalies.pbtxt`. 
      schema: A Schema protobuf describing the expected schema.
      baseline_stats: Baseline statistics to compare against.
      pipeline_options: Optional beam pipeline options. This allows users to
        specify various beam pipeline execution parameters like pipeline runner
        (DirectRunner or DataflowRunner), cloud dataflow service project id, etc.
        See https://cloud.google.com/dataflow/pipelines/specifying-exec-params for
        more details.
    """

    sampling_query_template = """
       SELECT *
       FROM 
           `{{ source_table }}` AS cover
       WHERE time BETWEEN '{{ start_time }}' AND '{{ end_time }}'
       """

    query = Template(sampling_query_template).render(
        source_table=table_name, start_time=start_time, end_time=end_time)

    stats_output_path = os.path.join(output_path, _STATS_FILENAME)
    anomalies_output_path = os.path.join(output_path, _ANOMALIES_FILENAME)

    with beam.Pipeline(options=pipeline_options) as p:
        raw_examples = (p
                        | 'GetData' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True)))

        examples = (raw_examples
                    | 'InstancesToBeamExamples' >> beam.ParDo(InstanceCoder(schema)))

        stats = (examples
                 | 'BeamExamplesToArrow' >> utils.batch_util.BatchExamplesToArrowRecordBatches()
                 | 'GenerateStatistics' >> GenerateStatistics()
                 )

        _ = (stats
             | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                 file_path_prefix=stats_output_path,
                 shard_name_template='',
                 coder=beam.coders.ProtoCoder(
                     statistics_pb2.DatasetFeatureStatisticsList)))

        _ = (stats
             | 'ValidateStatistics' >> beam.Map(validate_statistics, schema=schema)
             | 'WriteAnomaliesOutput' >> beam.io.textio.WriteToText(
                 file_path_prefix=anomalies_output_path,
                 shard_name_template='',
                 append_trailing_newlines=False))
