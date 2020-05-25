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

from typing import List, Optional, Text, Union, Dict, Iterable, Mapping
from tensorflow_data_validation import types
from tensorflow_data_validation import constants
from tensorflow_metadata.proto.v0 import schema_pb2

_RAW_DATA_COLUMN = 'raw_data'
_INSTANCES_KEY = 'instances'

_SCHEMA_TO_NUMPY = {
  schema_pb2.FeatureType.BYTES:  np.str,
  schema_pb2.FeatureType.INT: np.int64,
  schema_pb2.FeatureType.FLOAT: np.float
}
    
@beam.typehints.with_input_types(Dict)
@beam.typehints.with_output_types(types.BeamExample)
class InstanceCoder(beam.DoFn):
  """A DoFn which converts an AI Platform Prediction request body to
  types.BeamExample elements."""

  def __init__(self, schema: schema_pb2):

    self._example_size = beam.metrics.Metrics.counter(
      constants.METRICS_NAMESPACE, "example_size")

    self._features = {}
    for feature in schema.feature:
      if not feature.type in _SCHEMA_TO_NUMPY.keys():
        raise ValueError("Unsupported feature type: {}".format(feature.type))
      self._features[feature.name] = _SCHEMA_TO_NUMPY[feature.type]
    

  def process(self, log_record: Dict):

    _validate_request_response_log_schema(log_record)

    raw_data = json.loads(log_record[_RAW_DATA_COLUMN])

    if type(raw_data[_INSTANCES_KEY][0]) is dict:
      for instance in raw_data[_INSTANCES_KEY]:
        for name, value in instance.items():
          instance[name] = np.array(value, dtype=self._features[name])
        yield instance
    elif type(raw_data[_INSTANCES_KEY][0]) is list:
      for instance in raw_data[_INSTANCES_KEY]:
        yield {name: np.array([value], dtype=self._features[name])
          for name, value in zip(list(self._features.keys()), instance)}
    else:
      raise TypeError("Unsupported input instance format. Only JSON list or JSON object instances are supported")
        
  