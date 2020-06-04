#--runner=dataflow \
#--staging_location=gs://mlops-dev-workspace/beam \
#--temp_location=gs://mlops-dev-workspace/beam \
#--setup_file=./setup.py \
python run.py \
--project=mlops-dev-env \
--request_response_log_table=data_validation.test1 \
--model=covertype_tf \
--version=v3 \
--start_time=2020-05-25T16:01:10 \
--end_time=2020-05-25T22:50:30 \
--output_path=gs://mlops-dev-workspace/drift-monitor/output/test \
--schema_file=gs://mlops-dev-workspace/drift-monitor/schema/schema.pbtxt \
--baseline_stats_file=gs://mlops-dev-workspace/drift-monitor/baseline_stats/stats.pbtxt \
--time_window=60m
