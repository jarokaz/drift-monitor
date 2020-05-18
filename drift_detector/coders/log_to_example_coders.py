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

"""A DoFn converting a raw_data field in AI Platform Prediction
request-response log into tfdv.types.BeamExample.
"""

import json
import apache_beam as beam
import numpy as np
import tensorflow as tf

from typing import List, Optional, Text, Union, Dict, Iterable, Mapping
from tensorflow_data_validation import constants

_RAW_DATA_COLUMN = 'raw_data'
_INSTANCES_KEY = 'instances'
_LOGGING_TABLE_SCHEMA = {
  'model': lambda x: type(x) is str,
  'model_version': lambda x: type(x) is str,
  'time': lambda x: lambda x: type(x) is str,
  'raw_data': lambda x: type(x) is str, 
  'raw_prediction': lambda x: type(x) is str,
  'groundtruth': lambda x: type(x) is str
}


def _serialize_example(feature_dict: Dict) -> tf.train.Example:
  example = tf.train.Example()

  for name, values in feature_dict.items():
    feature = example.features.feature[name]
    if isinstance(values[0], str):
      values = [value.encode() for value in values]
      add = feature.bytes_list.value.extend
    elif isinstance(values[0], float):
      add = feature.float32_list.value.extend
    elif isinstance(values[0], int):
      add = feature.int64_list.value.extend
    else:
      raise AssertionError('Unsupported type: %s' % type(values[0]))
    add(np.array(values))

  return example.SerializeToString()


def _validate_request_response_log_schema(log_record: Dict):
    """Validates that log record conforms to schema."""
    
    incorrect_features = set(log_record.keys()) - set(_LOGGING_TABLE_SCHEMA.keys())
    if bool(incorrect_features):
      raise TypeError("Received log record with incorrect features %s" %
                       incorrect_features)
       
    features_with_wrong_type = [key for key, value in log_record.items() 
                                if not _LOGGING_TABLE_SCHEMA[key](value)]
    if bool(features_with_wrong_type):
      raise TypeError("Received log record with incorrect feature types %s" %
                       features_with_wrong_type)
    
    
@beam.typehints.with_input_types(Dict)
@beam.typehints.with_output_types(bytes)
class JSONObjectCoder(beam.DoFn):
  """A DoFn which converts an AI Platform Prediction input with instances in 
  a JSON object format to tf.Example elements."""

  def __init__(self):
    self._example_size = beam.metrics.Metrics.counter(
      constants.METRICS_NAMESPACE, "example_size")
      

  def process(self, log_record: Dict):
    
    _validate_request_response_log_schema(log_record)
 
    raw_data = json.loads(log_record[_RAW_DATA_COLUMN])
    if not type(raw_data[_INSTANCES_KEY][0]) is dict:
        raise TypeError("Expected instances in a JSON object format.")

    for instance in raw_data[_INSTANCES_KEY]:
        yield _serialize_example(instance)
            
      

@beam.typehints.with_input_types(Dict)
@beam.typehints.with_output_types(bytes)
class SimpleListCoder(beam.DoFn):
  """A DoFn which converts an AI Platform Prediction input with instances in 
  a simple list format to tf.Example elements."""

  def __init__(self, feature_names=None):
    self._example_size = beam.metrics.Metrics.counter(
      constants.METRICS_NAMESPACE, "example_size")
    
    self._feature_names = feature_names
      

  def process(self, log_record: Dict):
    
    _validate_request_response_log_schema(log_record)
            
    raw_data = json.loads(log_record[_RAW_DATA_COLUMN])
    if not type(raw_data[_INSTANCES_KEY][0]) is list:
        raise TypeError("Expected instances in a simple list format.")
        
    if not self._feature_names:
        raise TypeError("Feature names are required for instances in a simple list format.")
    
    if len(self._feature_names) != len(raw_data[_INSTANCES_KEY][0]):
        raise TypeError("The provided feature list does not match the length of an instance.")
                
    for instance in raw_data[_INSTANCES_KEY]:
        instance =  {name: [value]
            for name, value in zip(self._feature_names, instance)}
        yield _serialize_example(instance)
            

    
    

    

