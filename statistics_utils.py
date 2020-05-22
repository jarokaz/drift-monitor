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

"""A set of utility functions to help with retrieving data from a statistics
protocol buffer.
"""

import pandas as pd

from google.protobuf.json_format import MessageToDict
from tensorflow_metadata.proto.v0 import statistics_pb2
from tensorflow_data_validation import FeaturePath
from tensorflow_data_validation import get_slice_stats
from tensorflow_data_validation.utils.stats_util import get_feature_stats
from typing import List, Optional, Text, Union, Dict, Iterable, Mapping


def get_num_feature_stats_as_dataframe(
    stats_list: statistics_pb2.DatasetFeatureStatisticsList,
    feature_path: FeaturePath):
    """Returns a series of numeric statistics for a given
    feature formatted as a tidy dataframe."""
    
    feature_stats_list = []
    for dataset in stats_list.datasets:
        if dataset.name != 'All Examples':
            feature_stats = get_feature_stats(dataset, feature_path)
            if not feature_stats.HasField('num_stats'):
                raise ValueError('This is not a numeric feature')
            stats_dict = MessageToDict(feature_stats.num_stats)
            del stats_dict['commonStats']
            del stats_dict['histograms']
            stats_dict['slice'] = dataset.name
            feature_stats_list.append(stats_dict)

    return pd.DataFrame(feature_stats_list)


def get_histograms_as_dataframe(
    stats_list: statistics_pb2.DatasetFeatureStatisticsList,
    feature_path: FeaturePath):
    """Returns a series of histograms for a given numeric feature
    formatted as a tidy dataframe"""     

    buckets = []
    for dataset in stats_list.datasets:
        if dataset.name != 'All Examples':
            feature_stats = get_feature_stats(dataset, feature_path)
            if not feature_stats.HasField('num_stats'):
                raise ValueError('This is not a numeric feature')
            for histogram in feature_stats.num_stats.histograms:
                if histogram.type == statistics_pb2.Histogram.HistogramType.STANDARD:
                    for bucket in histogram.buckets:
                        bucket_dict = MessageToDict(bucket)
                        bucket_dict['slice'] = dataset.name
                        buckets.append(bucket_dict)

    return pd.DataFrame(buckets)
