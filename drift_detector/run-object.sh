REQUEST_RESPONSE_LOG_TABLE=mlops-dev-env.data_validation.covertype_classifier_logs_tf
FEATURE_NAMES=\
Elevation,\
Aspect,\
Slope,\
Horizontal_Distance_To_Hydrology,\
Vertical_Distance_To_Hydrology,\
Horizontal_Distance_To_Roadways,\
Hillshade_9am,\
Hillshade_Noon,\
Hillshade_3pm,\
Horizontal_Distance_To_Fire_Points,\
Wilderness_Area,\
Soil_Type
START_TIME=2020-05-09T5:05:14
END_TIME=2020-05-09T18:05:14
OUTPUT_PATH=gs://mlops-dev-workspace/drift_monitor/output/tf
DATAFLOW_GCS_LOCATION=gs://mlops-dev-workspace/drift_monitor/dataflow
SCHEMA_FILE=gs://mlops-dev-workspace/drift_monitor/schema/schema.pbtxt
BASELINE_STATS_FILE=gs://mlops-dev-workspace/drift_monitor/baseline_stats/stats.pb
INSTANCE_TYPE=OBJECT

PROJECT_ID=mlops-dev-env
RUNNER=DataflowRunner
SETUP_FILE=./setup.py

python run.py --project $PROJECT_ID \
--runner $RUNNER \
--staging_location $DATAFLOW_GCS_LOCATION/staging \
--temp_location $DATAFLOW_GCS_LOCATION/temp \
--setup_file $SETUP_FILE \
--request_response_log_table $REQUEST_RESPONSE_LOG_TABLE \
--instance_type $INSTANCE_TYPE \
--start_time $START_TIME \
--end_time $END_TIME \
--output_path $OUTPUT_PATH \
--schema_file $SCHEMA_FILE

