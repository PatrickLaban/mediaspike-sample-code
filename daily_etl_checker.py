from models.MetaData import MetaData
from mapreduce import mapreduce_pipeline
from models.DataStoreManager import DataStoreBase
from models.BaseRequestHandler import BaseRequestHandler
import os
import jinja2
import time
from models.CommonFunctions import email_task
import datetime

class EtlStatusHandler(BaseRequestHandler):
    """
    Checks on the status of all Etl Crons that ran the previous day.
    Sends an email to admins if any were aborted or have not completed.
    
    """
    
    def get(self):
        aborted = []
        incomplete = []
        today = datetime.date.today()
        compare_date = datetime.datetime(today.year, today.month, today.day) - datetime.timedelta(days=1)
        query = MetaData.query(MetaData.name == 'ETL mapreduce job', MetaData.created > compare_date) # Retrieve all ETL crons from previous day.
        for result in query:
            if result.created.date() == datetime.date.today():
                continue
            pipeline_id = result.values[0]
            pipeline = mapreduce_pipeline.MapreducePipeline.from_id(pipeline_id) #@UndefinedVariable
            if pipeline.has_finalized:
                if pipeline.was_aborted:
                    aborted.append(pipeline_id)
            else:
                incomplete.append(result.values[0])
        if len(aborted) > 0 or len(incomplete) > 0:
            template_values = {'aborted': aborted, 'incomplete': incomplete, 'host': os.environ['HTTP_HOST']}
            sender_address = DataStoreBase.ADMIN_EMAIL
            to_address = DataStoreBase.SUPPORT_EMAIL
            template_name = 'views/email/crons/etl_status_alert.html'
            
            jinja_environment = self.request.registry['jinja_environment']
            
            html_template = jinja_environment.get_template(template_name)
            html_body = html_template.render(template_values)
            template_name = template_name[:-5]
            plaintext_template = jinja_environment.get_template('{template_name}_plaintext.html'.format(template_name=template_name))
            plaintext_body = plaintext_template.render(template_values)
            task_name = "_".join(["admin", "etl_status", str(int(time.time()))])
            email_task(task_name, sender_address, to_address, 'ETL error alert.', plaintext_body, html_body=html_body)
        
