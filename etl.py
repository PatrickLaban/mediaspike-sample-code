from apiclient.discovery import build
from google.appengine.api import files # files is used for writing to Google Storage -  pylint: disable-msg=W0611
from google.appengine.api import logservice # needed to read in the logs - pylint: disable-msg=W0611
from mapreduce import base_handler, mapreduce_pipeline
from mapreduce.lib import pipeline
from models.MetaData import MetaData
from oauth2client.appengine import AppAssertionCredentials
from os import environ
import StringIO
import csv
import httplib2
import os
import time
import webapp2
import logging
import re

# Note many pylint messages being disabled due to the complexity of mapreduce

project_id = 'PROJECT_ID_PLACEHOLDER'
bigquery_dataset_id = 'Platform'
if re.search("platform200", environ['HTTP_HOST']) or re.search("mediaspikestaging", environ['HTTP_HOST']):
    gs_bucket_name = 'platform200'
    bigquery_table_id = "testing_analytics_v2"
    bigquery_schema = [{"name": "event_time", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "uuid", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "adunit_id", "type": "INTEGER", "mode": "NULLABLE"},
                       {"name": "supported_inventory_id", "type": "INTEGER", "mode": "NULLABLE"},
                       {"name": "gender", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "locale", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "interaction_type", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "interaction_count", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "view_time", "type": "INTEGER", "mode": "REQUIRED"},
                       {"name": "ip_address", "type": "STRING", "mode": "REQUIRED"},
                       {"name": "generic_1", "type": "INTEGER", "mode": "NULLABLE"},
                       {"name": "generic_2", "type": "INTEGER", "mode": "NULLABLE"},
                       {"name": "generic_3", "type": "INTEGER", "mode": "NULLABLE"},
                       ]
elif re.search("mediaspike1", environ['HTTP_HOST']) or re.search("mediaspike.com", environ['HTTP_HOST']):
    gs_bucket_name = 'mediaspike_analytics'
    bigquery_table_id = "analytics_v2"
    bigquery_schema = [{"name": "event_time", "type": "INTEGER"},
                       {"name": "uuid", "type": "STRING"},
                       {"name": "adunit_id", "type": "INTEGER"},
                       {"name": "supported_inventory_id", "type": "INTEGER"},
                       {"name": "gender", "type": "INTEGER"},
                       {"name": "age", "type": "INTEGER"},
                       {"name": "locale", "type": "STRING"},
                       {"name": "interaction_type", "type": "INTEGER"},
                       {"name": "interaction_count", "type": "INTEGER"},
                       {"name": "view_time", "type": "INTEGER"},
                       {"name": "ip_address", "type": "STRING"},
                       {"name": "generic_1", "type": "INTEGER"},
                       {"name": "generic_2", "type": "INTEGER"},
                       {"name": "generic_3", "type": "INTEGER"},
                       ]
else:
    gs_bucket_name = None
    bigquery_table_id = None
    bigquery_schema = None


credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')
http = credentials.authorize(http=httplib2.Http())
service = build('bigquery', 'v2', http=http)

class Log2Bq(base_handler.PipelineBase): # Method abstract but not not overridden - pylint: disable-msg=W0223
    """A pipeline to ingest log as CSV in Google Big Query."""
    def run(self, start_time, end_time, version_ids): # arguments number differs from overridden method - pylint: disable-msg=W0221
        files = yield Log2Gs(start_time, end_time, version_ids) # redefining name files - pylint: disable-msg=W0621
        yield Gs2Bq(files)

class Log2Gs(base_handler.PipelineBase):  # Method abstract but not not overridden - pylint: disable-msg=W0223
    """A pipeline to ingest log as CSV in Google Storage."""
    def run(self, start_time, end_time, version_ids):  # arguments number differs from overridden method - pylint: disable-msg=W0221
        # Create a MapperPipeline w/ `LogInputReader`, `FileOutputWriter`
        yield mapreduce_pipeline.MapperPipeline(
            "log2bq",
            "scripts.batch_module_crons.etl.log2csv",
            "mapreduce.input_readers.LogInputReader",
            "mapreduce.output_writers.FileOutputWriter",
            params={
                "input_reader" : {
                    "start_time": start_time,
                    "end_time": end_time,
                    "version_ids": version_ids,
                    "include_app_logs": True
                    },
                "output_writer" : {
                    "filesystem": "gs",
                    "gs_bucket_name": gs_bucket_name,
                    }
            },
            shards=16)

# Create a mapper function that convert request logs object to CSV.
def log2csv(r):
    """Convert log API RequestLog object to csv."""
    s = StringIO.StringIO()
    w = csv.writer(s)
    for app_log in r.app_logs:
        event = app_log.message
        if ',' in event:
            # Bad Event
            continue
        event = event.split('|')
        if len(event) != 13:
            # Bad Event
            continue
        event.insert(10, r.ip)
        try:
            # Validate types
            int(event[0])
            str(event[1])
            if event[2] != '':
                int(event[2])
            if event[3] != '':
                int(event[3])
            #int(event[3])
            int(event[4])
            int(event[5])
            str(event[6])
            int(event[7])
            int(event[8])
            int(event[9])
            str(event[10])
            for ev in event[13:]:
                if ev != '':
                    int(ev)
        except ValueError:
            continue
        w.writerow(event)
    lines = s.getvalue()
    s.close()
    yield lines

# Create a pipeline that takes gs:// files as argument and ingest them
# using a Big Query `load` job.
class Gs2Bq(base_handler.PipelineBase): # Method abstract but not not overridden - pylint: disable-msg=W0223
    """A pipeline to ingest log csv from Google Storage to Google BigQuery."""
    def run(self, files): # args number differ and redefining name files - pylint: disable-msg=W0221,W0621
        gs_paths = [f.replace('/gs/', 'gs://') for f in files]
        result = service.jobs().insert(projectId=project_id, # Incorrect error - pylint: disable-msg=E1101
            body={'projectId': project_id,
                  'configuration':{
                      'load':{
                          'sourceUris': gs_paths,
                          'schema': {
                              'fields': bigquery_schema,
                              },
                          'destinationTable': {
                              'projectId': project_id,
                              'datasetId': bigquery_dataset_id,
                              'tableId': bigquery_table_id
                          },
                          'createDisposition':'CREATE_IF_NEEDED',
                          'writeDisposition':'WRITE_APPEND',
                          'encoding':'UTF-8'
                      }}}).execute()
        yield BqCheck(result['jobReference']['jobId'])

# Create a pipeline that check for a Big Query job status
class BqCheck(base_handler.PipelineBase): # Method abstract but not not overridden - pylint: disable-msg=W0223
    """A pipeline to check for Big Query job status."""
    def run(self, job): # Args differ from overridden method - pylint: disable-msg=W0221
        jobs = service.jobs() # Incorrect error - pylint: disable-msg=E1101
        status = jobs.get(projectId=project_id,
            jobId=job).execute()
        logging.debug("Got status %s for bq job %s", status, job)
        job_state = status['status']['state']
        if job_state == 'PENDING' or job_state == 'RUNNING':
            delay = yield pipeline.common.Delay(seconds=1)
            with pipeline.After(delay):
                yield BqCheck(job)
        else:
            yield pipeline.common.Return(status)

# Create an handler that launch the pipeline and redirect to the
# pipeline UI.
class EtlHandler(webapp2.RequestHandler):
    def get(self):
        if gs_bucket_name is not None:
            end_time = self.request.get('end_time', None)
            if end_time is None:
                now = time.time()
                # Get the last end_time
                try:
                    last_run = MetaData.query(MetaData.name == 'Last ETL run').fetch(1)[0]
                    start_time = last_run.values[0]
                except IndexError:
                    # No previous last run, get the last hour
                    # Should only ever occur once.
                    start_time = now - 60 * 60
                    last_run = MetaData(name='Last ETL run', keys=['end_time', 'pipeline_id'], values=[now, ''])
            else:
                now = float(end_time)
                start_time = float(self.request.get('start_time', None))
            version = self.request.get('version', None)
            if version is None:
                major = os.environ["CURRENT_VERSION_ID"].split(".")[0]
            else:
                major = version
            p = Log2Bq(start_time, now, [major])
            p.start()
            meta_data = MetaData(name='ETL mapreduce job', keys=['pipeline_id'], values=[p.root_pipeline_id])
            meta_data.put()
            if end_time is None:
                # Update the last_run meta data with the new end_time (now).
                last_run.values[0] = now
                last_run.values[1] = p.root_pipeline_id
                last_run.put()
            else:
                try:
                    manual_run = MetaData.query(MetaData.name == 'Manual ETL run').fetch(1)[0]
                except IndexError:
                    manual_run = MetaData(name='Manual ETL run', keys=['pipeline_id'], values=[''])
                manual_run.values[0] = p.root_pipeline_id
                manual_run.put()
            #self.redirect('/mapreduce/pipeline/status?root=%s' % p.root_pipeline_id)
        else:
            logging.error('GS Bucket name was None')
