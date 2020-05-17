REQUEST_RESPONSE_LOG_TABLE=mlops-dev-env.data_validation.covertype_classifier_logs_tf
INSTANCE_TYPE=OBJECT
START_TIME=2020-05-01T20:00:27
END_TIME=2020-05-01T20:01:39
OUTPUT_PATH=gs://hostedkfp-default-36un4wco1q/tfdv/output
DATAFLOW_GCS_LOCATION=gs://hostedkfp-default-36un4wco1q/tfdv/dataflow
SCHEMA_FILE=gs://hostedkfp-default-36un4wco1q/tfdv/schema/schema.pbtxt
BASELINE_STATS_FILE=gs://hostedkfp-default-36un4wco1q/tfdv/baseline_stats/stats.pb

PROJECT_ID=mlops-dev-env
TEMPLATE_PATH=gs://mlops-dev-workspace/dataflow_templates/drift_detector/drift_detector.json
SETUP_FILE=./setup.py

gcloud beta dataflow flex-template run "data-drift-`date +%Y%m%d-%H%M%S`" \
--template-file-gcs-location "$TEMPLATE_PATH" \
--parameters \
"^;^setup_file=$SETUP_FILE;\
instance_type=OBJECT;\
request_response_log_table=$REQUEST_RESPONSE_LOG_TABLE;\
start_time=$START_TIME;\
end_time=$END_TIME;\
output_path=$OUTPUT_PATH;\
schema_file=$SCHEMA_FILE"
