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

"""A PTransform converting dictionaries as returned by beam.io.BigQuerySource into pyarrow tables."""

import apache_beam as beam
import pyarrow

from typing import List, Optional, Text, Union, Dict, Iterable, Mapping
from tfx_bsl.tfxio import record_based_tfxio

# BigQuery to Arrow type mappings
_BQ_TO_ARROW = {
    'STRING': pyarrow.list_(pyarrow.binary()),
    'INTEGER': pyarrow.list_(pyarrow.int64()),
    'FLOAT': pyarrow.list_(pyarrow.float32()),
    'BOOLEAN': pyarrow.list_(pyarrow.int64())
}

def validate_bq_types(types: List[Text]) -> bool:
    """Validates whether a set of BQ type is supported by DecodeBigQuery transform."""
    
    return set(types).issubset(_BQ_TO_ARROW.keys())

class RecordsToTable(beam.DoFn):
    """DoFn to convert a batch of records from beam.io.BigQuerySource
    to a pyarrow.Table."""

    def process(self, batch: List[Mapping], 
                column_specs: Mapping) -> Iterable[pyarrow.Table]:
        
        column_names = batch[0].keys()
        # Check that the batch conforms to schema
        if set(column_names) != set(column_specs.keys()):
            raise ValueError("Columns in a batch don't match required column specs")
        
        values_by_column = {column_name: [] for column_name in column_names}
        
        for row in batch:
            for key, value in row.items():
                values_by_column[key].append([value])
                
        # TODO: Validate whether pa.array should receive a list of one large
        # sublist or a set of one element sublists
        arrow_arrays = [
            #pyarrow.array(arr, type=_BQ_TO_ARROW[column_specs[column_name]]) 
            pyarrow.array(arr) 
            for column_name, arr in values_by_column.items()
            ]  
        
        yield pyarrow.Table.from_arrays(arrow_arrays, list(column_names))
  

class DecodeBigQuery(beam.PTransform):
    """Decodes BigQuery records into Arrow Tables7."""
    def __init__(
        self,
        column_specs: Mapping,
        desired_batch_size: int):
    
        self._column_specs = column_specs
        self._desired_batch_size = desired_batch_size
        
    def expand(self, pcoll: beam.pvalue.PCollection):
        record_batches = (
            pcoll
            | beam.BatchElements(
                  min_batch_size = self._desired_batch_size,
                  max_batch_size = self._desired_batch_size)
            | beam.ParDo(
                  RecordsToTable(), self._column_specs)
            )

        return record_batches
