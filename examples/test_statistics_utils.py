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

import tensorflow_data_validation as tfdv
from tensorflow_data_validation import FeaturePath

from statistics_utils import get_num_feature_stats_as_dataframe
from statistics_utils import get_histograms_as_dataframe

@pytest.fixture
def stats_list():
    local_workspace = '/home/jarekk/workspace/analysis'
    local_tfrecords_file = '{}/log_records.tfrecords'.format(local_workspace)

    slice_fn = tfdv.get_feature_value_slicer(features={'time_window': None})
    stats_options = tfdv.StatsOptions( slice_functions=[slice_fn])

    stats_list = tfdv.generate_statistics_from_tfrecord(
        data_location=local_tfrecords_file,
        stats_options=stats_options
    )
    return stats_list

def test_get_num_feature_stats_as_dataframe(stats_list):
    feature_path = FeaturePath(['Slope'])

    df = get_num_feature_stats_as_dataframe(stats_list, feature_path)
    print(df)

def test_get_histograms_as_dataframe(stats_list):
    feature_path = FeaturePath(['Slope'])

    df = get_histograms_as_dataframe(stats_list, feature_path)
    print(df)