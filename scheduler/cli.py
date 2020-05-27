# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A command line interface to trigger and schedule drift detector runs.  """

import click
import logging

from handlers import run_drift_detector
from handlers import schedule_drift_detector

#@click.command()
#@click.option('--project', 'project_id', help='A GCP project ID', required=True)
#@click.option('--region', help='A GCP region', required=True)
#@click.option('--queue', 'task_queue', help='A Cloud Tasks queue to use for scheduling', required=True)
#@click.option('--account', 'service_account', help='The service account to be used by runs', required=True)
#@click.option('--template_path', help='A path to the Dataflow template', required=True)
#@click.option('--beginning_time', help='A beginning of the first time window using UTC time.', required=True, type=click.DateTime())
#@click.option('--time_window', help='Length of the time window', required=True, type=int)
#@click.option('--num_of_runs', help='A number of runs', required=True, type=int)
#@click.option('--log_table', 'request_response_log_table', help='A full name of the request_response log table', required=True)
#@click.option('--output', 'output_root_folder', help='A GCS location for the output statistics and anomalies files', required=True)
#@click.option('--schema', 'schema_file', help='A GCS location of the schema file', required=True)
#@click.option('--stats', 'baseline_stats_file', help='A GCS location of the baseline stats file')

@click.group()
def cli():
    pass


@cli.command()
@click.argument('template_path')
@click.option('--project', envvar='DM_PROJECT_ID', help='A GCP project ID', required=True)
@click.option('--region', envvar='DM_REGION', help='A GCP region', required=True)
@click.option('--log_table', envvar='DM_LOG_TABLE', help='A full name of the request_response log table', required=True)
@click.option('--start_time', envvar='DM_START_TIME', help='The beginning of a time window in the log table (UTC time).', required=True, type=click.DateTime())
@click.option('--end_time', envvar='DM_END_TIME', help='The end of a time window in the log table (UTC time).', required=True, type=click.DateTime())
@click.option('--output', envvar='DM_OUTPUT', help='A GCS location for the output statistics and anomalies files', required=True)
@click.option('--schema',  envvar='DM_SCHEMA', help='A GCS location of the schema file', required=True)
@click.option('--baseline_stats', envvar='DM_STATS', help='A GCS location of the baseline stats file')
def run(template_path,
    project,
    region,
    log_table,
    start_time,
    end_time,
    output,
    schema,
    baseline_stats
):
    response = run_drift_detector(
        project_id=project,
        region=region,
        template_path=template_path,
        log_table=log_table,
        start_time=start_time,
        end_time=end_time,
        output_location=output,
        schema_location=schema,
        baseline_stats_location=baseline_stats
    )
    logging.log(logging.INFO, "Submited a drift detector template run: DataFlow Job ID={}".format(
        response['job']['id'])) 

@cli.command()
def schedule():
    print('schedule')

#@cli.group()
#def monitors():
#    pass
#
#@monitors.command()
#def add():
#    print('add')
#
#@monitors.command()
#def list():
#    print('list')
#
#@monitors.command()
#def describe():
#    print('describe')
#
#@cli.group()
#def reports():
#    pass
#
#@reports.command()
#def run():
#    print('run')
#
#@reports.command()
#def schedule():
#    print('schedule')
#
#@reports.command()
#def list_scheduled():
#    print('List_scheduled')
#
#@reports.command()
#def list_available():
#    print('List_available')
#
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    cli()