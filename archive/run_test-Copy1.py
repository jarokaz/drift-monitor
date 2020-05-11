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



# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging
import os

from typing import List, Optional, Text, Union, Dict, Iterable

import apache_beam as beam
import pyarrow as pa
import tensorflow_data_validation as tfdv

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners import DirectRunner

from google.protobuf import text_format
from tensorflow_data_validation.statistics import stats_options
from tensorflow_data_validation.utils import io_util
from tensorflow_data_validation.utils import batch_util
from tensorflow_metadata.proto.v0 import statistics_pb2

#from utils.gen_stats import generate_statistics_from_bq



FeatureName = Union[bytes, Text]
FeatureValue = List[Union[int, str, float, bool, None]]
FeatureDict = Dict[FeatureName, FeatureValue]

def _get_empty_table(num_rows) -> pa.Table:
  # This is to mitigate that pyarrow doesn't provide an API 
  # to create a table with zero column but non zero rows. 
  
  t = pa.Table.from_arrays(
      [pa.array([None] * num_rows, type=pa.null())], ["dummy"])
  return t.remove_column(0)

@beam.typehints.with_input_types(List[FeatureDict])
@beam.typehints.with_output_types(pa.Table)
class BatchedDictsToArrowTable(beam.DoFn):
    """A DoFn to convert a batch of input instances in a feature
    dictionary format to an Arrow table. 
    """

    def process(self, 
                batch: List[FeatureDict]
                ) -> pa.Table:
        
        """
        values_by_column = [[] for _ self._column_names]
        
        for instance in batch:
            for key, value in row.items():
                values_by_column[key].append((value,))
                
        arrow_arrays = [
            pyarrow.array(arr, type=_BQ_TO_ARROW[column_specs[column_name]]) 
            for column_name, arr in values_by_column.items()
            ]  
        
        yield pyarrow.Table.from_arrays(arrow_arrays, list(column_names))
        """
        
        print('************')
        print(batch)
        print('************')
        
        if not batch:
            return _get_empty_table(0)
        
        struct_array = pa.array(batch)
        if not pa.types.is_struct(struct_array.type):
            raise ValueError("Unexpected Arrow type created from input")
            
        field_names = [f.name for f in list(struct_array.type)]
        if not field_names:
            return _get_empty_table(len(batch))
        
        value_arrays = struct_array.flatten()
        
        for name, array in zip(field_names, value_arrays):
            if pa.types.is_null(array.type):
                continue
                
            if (not pa.types.is_list(array.type) and
                not pa.types.is_large_list(array.type)):
                raise TypeError("Expected list arrays for field {} but got {}".format(
                    name, array.type))
                
            value_type = array.type.value_type
            if (not pa.types.is_integer(value_type) and
                not pa.types.is_floating(value_type) and
                not pa.types.is_binary(value_type) and
                not pa.types.is_large_binary(value_type) and
                not pa.types.is_unicode(value_type) and
                not pa.types.is_large_unicode(value_type) and
                not pa.types.is_null(value_type)):
                raise TypeError("Type not supported: {} {}".format(name, array.type))
                        
        print(value_arrays)
        
        
        yield pa.Table.from_arrays(value_arrays, field_names)
        
        
if __name__ == '__main__':
    # Cloud Logging would contain only logging.INFO and higher level logs logged
    # by the root logger. All log statements emitted by the root logger will be
    # visible in the Cloud Logging UI. Learn more at
    # https://cloud.google.com/logging about the Cloud Logging UI.
    #
    # You can set the default logging level to a different level when running
    # locally.
    logging.getLogger().setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_path',
        dest='output_path',
        help='Output path',
        #default = 'gs://hostedkfp-default-36un4wco1q/tfdv')
        default = '/home/jupyter/output')
    parser.add_argument(
        '--dataflow_gcs_location',
        dest='dataflow_gcs_location',
        help='A GCS root for Dataflow staging and temp locations.',
        default = 'gs://hostedkfp-default-36un4wco1q/dataflow')
    parser.add_argument(
        '--schema_file',
        dest='schema_file',
        help='A path to a schema file',
        default = 'schema.pbtxt')

    known_args, pipeline_args = parser.parse_known_args()
    
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % known_args.dataflow_gcs_location
    pipeline_options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % known_args.dataflow_gcs_location
    
    stats_options = stats_options.StatsOptions()
    schema = tfdv.load_schema_text(known_args.schema_file)
    
    anomalies_output_path = os.path.join(
        known_args.output_path, 'test.txt'
    )
    stats_output_path = os.path.join(
        known_args.output_path, 'stats.pb'
    )
    
    desired_batch_size = (
        stats_options.desired_batch_size if stats_options.desired_batch_size
            and stats_options.desired_batch_size > 0 else
            tfdv.constants.DEFAULT_DESIRED_INPUT_BATCH_SIZE)
    
    instances = [
        {'f1': [1], 'f2': [0.1], 'f3': ['aaa']},
        {'f1': [2], 'f2': [0.2], 'f3': ['bbb']},
        {'f1': [2], 'f2': [0.3], 'f3': ['ddd']},
        {'f1': [2], 'f2': [0.4], 'f3': ['bbb']},
        {'f1': [3],  'f3': ['bbb'], 'f4': [1]}
    ]
    
    with beam.Pipeline(options=pipeline_options) as p:
        stats = ( p
            | 'GetData' >> beam.Create(instances)
 #           | 'BatchDictionaries' >> beam.BatchElements(
 #                 min_batch_size = desired_batch_size,
 #                 max_batch_size = desired_batch_size)
 #           | 'CovertToArrowTables' >> beam.ParDo(
 #               BatchedDictsToArrowTable())
            | 'DecodeExamples' >> batch_util.BatchExamplesToArrowTables()
            | 'GenerateStatistics' >> tfdv.GenerateStatistics())
        
        
        _ = (stats       
            | 'WriteStatsOutput' >> beam.io.WriteToTFRecord(
                  file_path_prefix=stats_output_path,
                  shard_name_template='',
                  coder=beam.coders.ProtoCoder(
                      statistics_pb2.DatasetFeatureStatisticsList)))
   
       # _ = (stats
            #| 'ValidateStatistics' >> beam.Map(tfdv.validate_statistics, schema=schema)
        #    | 'WriteAnomaliesOutput' >> beam.io.textio.WriteToText(
          #                                  file_path_prefix=anomalies_output_path,
         #                                   shard_name_template='',
           #                                 append_trailing_newlines=True))
        