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
import numpy as np

import tensorflow as tf
from tensorflow_metadata.proto.v0 import schema_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson, ParseDict

from coders.beam_example_coders import InstanceCoder, _validate_request_response_log_schema

schema_dict = {
    'feature': [
        {'name': 'Soil_Type', 'type': 'BYTES', 'domain': 'Soil_Type'},
        {'name': 'Wilderness_Area', 'type': 'BYTES', 'domain': 'Wilderness_Area'},
        {'name': 'Aspect', 'type': 'INT'},
        {'name': 'Cover_Type', 'type': 'INT'},
        {'name': 'Elevation', 'type': 'INT'},
        {'name': 'Hillshade_3pm', 'type': 'INT'},
        {'name': 'Hillshade_9am', 'type': 'INT'},
        {'name': 'Hillshade_Noon', 'type': 'INT'},
        {'name': 'Horizontal_Distance_To_Fire_Points', 'type': 'INT'},
        {'name': 'Horizontal_Distance_To_Hydrology', 'type': 'INT'},
        {'name': 'Horizontal_Distance_To_Roadways', 'type': 'INT'},
        {'name': 'Slope', 'type': 'INT'},
        {'name': 'Vertical_Distance_To_Hydrology', 'type': 'INT'}],
    'stringDomain': [
        {'name': 'Soil_Type',
        'value': ['C2702', 'C2703', 'C2704', 'C2705', 'C2706', 'C2717',
                  'C3501', 'C3502', 'C4201', 'C4703', 'C4704', 'C4744',
                  'C4758', 'C5101', 'C5151', 'C6101', 'C6102', 'C6731',
                  'C7101', 'C7102', 'C7103', 'C7201', 'C7202', 'C7700',
                  'C7701', 'C7702', 'C7709', 'C7710', 'C7745', 'C7746',
                  'C7755', 'C7756', 'C7757', 'C7790', 'C8703', 'C8707',
                  'C8708', 'C8771', 'C8772', 'C8776']},
      {'name': 'Wilderness_Area',
        'value': ['Cache', 'Commanche', 'Neota', 'Rawah']}]}


_log_record_object_format = {
    "model": "covertype_classifier_tf",
    "model_version": "v2",
    "time": "2020-05-17 10:30:00 UTC",
    "raw_data": '{"instances": [{"Elevation": [3716, 3717], "Aspect": [336, 337], "Slope": [9, 8], "Horizontal_Distance_To_Hydrology": [1026, 1027], "Vertical_Distance_To_Hydrology": [270, 271], "Horizontal_Distance_To_Roadways": [5309, 5319], "Hillshade_9am": [203, 204], "Hillshade_Noon": [230, 231], "Hillshade_3pm": [166, 167], "Horizontal_Distance_To_Fire_Points": [3455, 3444], "Wilderness_Area": ["Commanche", "Aaaa"], "Soil_Type": ["8776", "9999"]}, {"Elevation": [3225], "Aspect": [326], "Slope": [9], "Horizontal_Distance_To_Hydrology": [342], "Vertical_Distance_To_Hydrology": [0], "Horizontal_Distance_To_Roadways": [5500], "Hillshade_9am": [198], "Hillshade_Noon": [230], "Hillshade_3pm": [172], "Horizontal_Distance_To_Fire_Points": [1725], "Wilderness_Area": ["Rawah"], "Soil_Type": ["7201"]}]}',
    "raw_prediction": '{"predictions": [[4.21827644e-06, 1.45283067e-07, 6.71478847e-21, 4.34945702e-21, 5.18628625e-31, 1.35843754e-22, 0.999995589], [0.948056221, 0.0518435165, 2.80540131e-12, 4.14544565e-14, 8.18011e-10, 1.02051131e-10, 0.000100270954]]}',
    "groundtruth": "NaN"
  }

_log_record_list_format = {
    "model": "covertype_classifier_sklearn",
    "model_version": "v2",
    "time": "2020-05-17 10:30:00 UTC",
    "raw_data": '{"instances": [[3012, 84, 7, 309, 50, 361, 230, 228, 131, 1205, "Rawah", "7202"], [3058, 181, 16, 42, 10, 1803, 224, 248, 152, 421, "Commanche", "4758"]]}',
    "raw_prediction": '{"predictions": [0, 1, 1]}',
    "groundtruth": "NaN"
  }


@pytest.fixture
def coder():
    schema = schema_pb2.Schema()
    ParseDict(schema_dict, schema)
    return InstanceCoder(schema=schema)

def test_instancecoder_constructor():
    expected_result = {
      'Soil_Type': np.str, 
      'Wilderness_Area': np.str,
      'Aspect': np.int64, 
      'Cover_Type': np.int64, 
      'Elevation': np.int64, 
      'Hillshade_3pm': np.int64, 
      'Hillshade_9am': np.int64,
      'Hillshade_Noon': np.int64, 
      'Horizontal_Distance_To_Fire_Points': np.int64, 
      'Horizontal_Distance_To_Hydrology': np.int64, 
      'Horizontal_Distance_To_Roadways': np.int64, 
      'Slope': np.int64, 
      'Vertical_Distance_To_Hydrology':np.int64}

    schema = schema_pb2.Schema()
    ParseDict(schema_dict, schema)
    coder = InstanceCoder(schema=schema)
    assert coder._features == expected_result


def test_instancecoder(coder):
    examples = coder.process(_log_record_object_format)
    example = next(examples)


def test_validate_request_response_log_schema():
    result = _validate_request_response_log_schema(_log_record_object_format)


def test_list_coder():
    coder = SimpleListCoder(_feature_names)
    examples = coder.process(_log_record_list_format)
    example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    example = {key: _convert_to_dense(value) for key, value in example.items()}
    print(example)
    example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    example = {key: _convert_to_dense(value) for key, value in example.items()}
    print(example)