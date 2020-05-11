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

""" A DoFn that coverts a batch of features into an Arrow table."""


import apache_beam as beam
import pyarrow as pa

from typing import Dict, List, Mapping, Union
from tensorflow_metadata.proto.v0 import schema_pb2
from tensorflow_metadata.proto.v0 import statistics_pb2

_ARROW_TYPE_MAP = {
    ColumnType.UNKNOWN: pa.null(),
    ColumnType.INT: pa.list_(pa.int64()),
    ColumnType.FLOAT: pa.list_(pa.float32()),
    ColumnType.STRING: pa.list_(pa.binary()),
}


SimpleFeatureList = List[Union[int, str, float, bool]]
ColumnName = Union[bytes, Text]

@beam.typehints.with_input_types(List[SimpleFeatureList])
@beam.typehints.with_output_types(pa.RecordBatch)
class BatchedFeatureListsToRecordBatch(beam.DoFn):
    """A DoFn to convert a batch of input instances in a feature
    list format to an Arrow table. 
    """
    
    def __init__(
        self,
        column_names: List[ColumnName]
    ):
    """Initializes the feature converter.
    
    Args:
        column_names: A list of column names. The order of the list must 
        match the order of the features in a feature list.
    """
    self._column_names = column_names

    def process(self, 
                batch: List[SimpleFeatureList]
                ) -> Iterable[pa.RecordBatch]:
        
        values_by_column = [[] for _ self._column_names]
        
        
        for instance in batch:
            for key, value in row.items():
                values_by_column[key].append((value,))
                
        # TODO: Validate whether pa.array should receive a list of one large
        # sublist or a set of one element sublists
        arrow_arrays = [
            pyarrow.array(arr, type=_BQ_TO_ARROW[column_specs[column_name]]) 
            for column_name, arr in values_by_column.items()
            ]  
        
        yield pyarrow.Table.from_arrays(arrow_arrays, list(column_names))
  