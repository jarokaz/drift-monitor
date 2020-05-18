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

from coders.log_to_example_coders import JSONObjectCoder, SimpleListCoder


_covertype_feature_description = {
    'Wilderness_Area': tf.io.VarLenFeature(tf.string),
    'Vertical_Distance_To_Hydrology': tf.io.VarLenFeature(tf.int64),
    'Soil_Type': tf.io.VarLenFeature(tf.string),
    'Hillshade_3pm': tf.io.VarLenFeature(tf.int64),
    'Hillshade_Noon': tf.io.VarLenFeature(tf.int64),
    'Horizontal_Distance_To_Fire_Points': tf.io.VarLenFeature(tf.int64),
    'Aspect': tf.io.VarLenFeature(tf.int64),
    'Hillshade_9am': tf.io.VarLenFeature(tf.int64),
    'Horizontal_Distance_To_Hydrology': tf.io.VarLenFeature(tf.int64),
    'Slope': tf.io.VarLenFeature(tf.int64),
    'Elevation': tf.io.VarLenFeature(tf.int64),
    'Horizontal_Distance_To_Roadways': tf.io.VarLenFeature(tf.int64),
}

_log_record_object_format = {
    "model": "covertype_classifier_tf",
    "model_version": "v2",
    "time": "2020-05-17 10:30:00 UTC",
    "raw_data": '{"instances": [{"Elevation": [3716, 3717], "Aspect": [336, 337], "Slope": [9, 8], "Horizontal_Distance_To_Hydrology": [1026, 1027], "Vertical_Distance_To_Hydrology": [270, 271], "Horizontal_Distance_To_Roadways": [5309, 5319], "Hillshade_9am": [203, 204], "Hillshade_Noon": [230, 231], "Hillshade_3pm": [166, 167], "Horizontal_Distance_To_Fire_Points": [3455, 3444], "Wilderness_Area": ["Commanche", "Aaaa"], "Soil_Type": ["8776", "9999"]}, {"Elevation": [3225], "Aspect": [326], "Slope": [9], "Horizontal_Distance_To_Hydrology": [342], "Vertical_Distance_To_Hydrology": [0], "Horizontal_Distance_To_Roadways": [5500], "Hillshade_9am": [198], "Hillshade_Noon": [230], "Hillshade_3pm": [172], "Horizontal_Distance_To_Fire_Points": [1725], "Wilderness_Area": ["Rawah"], "Soil_Type": ["7201"]}]}',
    "raw_prediction": '{"predictions": [[4.21827644e-06, 1.45283067e-07, 6.71478847e-21, 4.34945702e-21, 5.18628625e-31, 1.35843754e-22, 0.999995589], [0.948056221, 0.0518435165, 2.80540131e-12, 4.14544565e-14, 8.18011e-10, 1.02051131e-10, 0.000100270954]]}',
    "groundtruth": "null"
  }

_log_record_list_format = {
    "model": "covertype_classifier_sklearn",
    "model_version": "v2",
    "time": "2020-05-17 10:30:00 UTC",
    "raw_data": '{"instances": [{"Elevation": [3716, 3717], "Aspect": [336, 337], "Slope": [9, 8], "Horizontal_Distance_To_Hydrology": [1026, 1027], "Vertical_Distance_To_Hydrology": [270, 271], "Horizontal_Distance_To_Roadways": [5309, 5319], "Hillshade_9am": [203, 204], "Hillshade_Noon": [230, 231], "Hillshade_3pm": [166, 167], "Horizontal_Distance_To_Fire_Points": [3455, 3444], "Wilderness_Area": ["Commanche", "Aaaa"], "Soil_Type": ["8776", "9999"]}, {"Elevation": [3225], "Aspect": [326], "Slope": [9], "Horizontal_Distance_To_Hydrology": [342], "Vertical_Distance_To_Hydrology": [0], "Horizontal_Distance_To_Roadways": [5500], "Hillshade_9am": [198], "Hillshade_Noon": [230], "Hillshade_3pm": [172], "Horizontal_Distance_To_Fire_Points": [1725], "Wilderness_Area": ["Rawah"], "Soil_Type": ["7201"]}]}',
    "raw_prediction": '{"predictions": [[4.21827644e-06, 1.45283067e-07, 6.71478847e-21, 4.34945702e-21, 5.18628625e-31, 1.35843754e-22, 0.999995589], [0.948056221, 0.0518435165, 2.80540131e-12, 4.14544565e-14, 8.18011e-10, 1.02051131e-10, 0.000100270954]]}',
    "groundtruth": "null"
  }


def _convert_to_dense(x):
    default_value = '' if x.dtype == tf.string else 0
    return tf.sparse.to_dense(
          x, 
          default_value)

def test_object_coder():
    coder = JSONObjectCoder()
    examples = coder.process(_log_record_object_format)
    first_example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    print(first_example)
    second_example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    print(second_example)


def test_list_coder():
    coder = JSONObjectCoder()
    examples = coder.process(_log_record)
    first_example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    print(first_example)
    second_example = tf.io.parse_single_example(next(examples),
                                            _covertype_feature_description)
    print(second_example)