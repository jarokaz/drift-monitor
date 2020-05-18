import tensorflow_data_validation as tfdv
import pandas as pd
import datetime

from tensorflow_data_validation.utils import slicing_util


data_location= '/home/jarekk/workspace/test.csv'
output_location = '/home/jarekk/workspace/stats.pb'

slice_fn = slicing_util.get_feature_value_slicer(features={'time_window':None})

stats_options = tfdv.StatsOptions(
    slice_functions=[slice_fn]
)

stats = tfdv.generate_statistics_from_csv(data_location,
   stats_options=stats_options,
   output_path=output_location
   )