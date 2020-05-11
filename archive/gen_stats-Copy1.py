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
"""A utility function to generate statistics from BigQuery query"""

import os
import logging
from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import tensorflow_data_validation as tfdv

from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from tensorflow_data_validation.statistics import stats_options
from tensorflow_data_validation.utils import batch_util

from tensorflow_metadata.proto.v0 import statistics_pb2
from tensorflow_metadata.proto.v0 import schema_pb2

from coders.bq_encoder import DecodeBigQuery
from coders.bq_encoder import validate_bq_types

_STATS_FILENAME='stats.pb'
_ANOMALIES_FILENAME='anomalies.pbtxt'

def _get_column_specs(query) -> Dict:
    """Gets column specs for a result a BQ query."""
    
    client = bigquery.Client()
    
    query_job = client.query('SELECT * FROM ({}) LIMIT 0'.format(query))
    results = query_job.result()
    column_specs = {field.name: field.field_type for field in results.schema}
    
    return column_specs

def generate_statistics_from_bq(
        query: Text,
        output_path: Text,
        schema: schema_pb2.Schema,
        stats_options: stats_options.StatsOptions = stats_options.StatsOptions(),
        pipeline_options: Optional[PipelineOptions] = None,       
) -> statistics_pb2.DatasetFeatureStatisticsList:
    """Computes data statistics from a BigQuery query result.
  
    Args:
      query: The BigQuery query.
      output_path: The file path to output data statistics result to. 
        It will be a TFRecord file containing a single
        data statistics proto, and can be read with the 'load_statistics' API.
        If you run this function on Google Cloud, you must specify an
        output_path. Specifying None may cause an error.
      schema: A Schema protobuf to use for data validation
      stats_options: `tfdv.StatsOptions` for generating data statistics.
      pipeline_options: Optional beam pipeline options. This allows users to
        specify various beam pipeline execution parameters like pipeline runner
        (DirectRunner or DataflowRunner), cloud dataflow service project id, etc.
        See https://cloud.google.com/dataflow/pipelines/specifying-exec-params for
        more details.
    Returns:
      A DatasetFeatureStatisticsList proto.
    """
  
    column_specs = _get_column_specs(query)
    if not validate_bq_types(_get_column_specs(query).values()):
        raise ValueError("Unsupported BigQuery data types.")
        
    batch_size = (
        stats_options.desired_batch_size if stats_options.desired_batch_size
            and stats_options.desired_batch_size > 0 else
            tfdv.constants.DEFAULT_DESIRED_INPUT_BATCH_SIZE)
    # PyLint doesn't understand Beam PTransforms.
    # pylint: disable=no-value-for-parameter
    
    stats_output_path = os.path.join(output_path, _STATS_FILENAME)
    anomalies_output_path = os.path.join(output_path, _ANOMALIES_FILENAME)
 
    with beam.Pipeline(options=pipeline_options) as p:
        stats = ( p
            | 'GetData' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
    #        | 'DecodeData' >>  DecodeBigQuery(column_specs,
    #                                          desired_batch_size=batch_size)
            | 'DecodeExamples' >> batch_util.BatchExamplesToArrowTables()
            | 'GenerateStatistics' >> tfdv.GenerateStatistics())
        
        _ = (stats       
            | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                  file_path_prefix=stats_output_path,
                  shard_name_template='',
                  coder=beam.coders.ProtoCoder(
                      statistics_pb2.DatasetFeatureStatisticsList)))
        _ = (stats
            | 'ValidateStatistics' >> beam.Map(tfdv.validate_statistics, schema=schema)
            | 'WriteAnomaliesOutput' >> beam.io.textio.WriteToText(
                                            file_path_prefix=anomalies_output_path,
                                            shard_name_template='',
                                            append_trailing_newlines=False))
        

    

