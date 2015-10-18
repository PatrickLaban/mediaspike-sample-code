from google.appengine.ext import ndb
from models.BaseRequestHandler import BaseRequestHandler
from models.CommonFunctions import batch_big_query
from models.DataStoreManager import ModelState, IntegrationLive
from models.Application import Application
from os import environ
import time 
import logging


class ApplicationStatusCronHandler(BaseRequestHandler):
    @ndb.toplevel
    def get(self):
        # Detect the current environment and set the appropriate table name
        if environ['HTTP_HOST'] == "platform200.appspot.com" or environ['HTTP_HOST'] == 'mediaspikestaging.appspot.com':
            table_name = "Platform.testing_analytics_v2"
        elif environ['HTTP_HOST'] == "mediaspike1.appspot.com" or environ['HTTP_HOST'] == "www.mediaspike.com":
            table_name = "Platform.analytics_v2"
        else:
            table_name = None
        if table_name is not None:
            now = time.time()
            day_start_time = now - (now % 86400) # Get the start of the previous day - 86400=60*60*24 
            end_time = day_start_time + 86400 # Add 24 hours
            month_start_time = end_time - (86400 * 30) # Subtract 30 days
            applications = [application for application in Application.query(Application.state == ModelState.ACTIVE)]
            active_application_ids = [str(application.get_key_id()) for application in applications]
            id_string = ','.join(active_application_ids)
            status_dau_query_string = """
                                      SELECT generic_2, COUNT(event_time), COUNT(DISTINCT(uuid))
                                      FROM {table_name}
                                      WHERE generic_2 IN ({ids})
                                      AND event_time BETWEEN {start_time} AND {end_time}
                                      AND interaction_type = 1
                                      GROUP BY generic_2;
                                      """.format(table_name=table_name,
                                                 ids=id_string,
                                                 start_time=day_start_time*1000000,
                                                 end_time=end_time*1000000)
            mau_query_string = """
                               SELECT supported_inventory_id, COUNT(DISTINCT(uuid))
                               FROM {table_name}
                               WHERE supported_inventory_id IN ({ids})
                               AND event_time BETWEEN {start_time} AND {end_time}
                               AND interaction_type = 1
                               GROUP BY supported_inventory_id;
                               """.format(table_name=table_name,
                                          ids=id_string,
                                          start_time=month_start_time*1000000,
                                          end_time=end_time*1000000)
            query_list = [status_dau_query_string, mau_query_string]
            batch_query_results = batch_big_query(query_list)
            query_results = {int(result[0]): [int(result[1]), int(result[2])] for result in batch_query_results[0]}
            for result in batch_query_results[1]:
                query_results[int(result[0])].append(int(result[1]))
            for application_index, application_id in enumerate(active_application_ids):
                # See if the result is considered live - greater than 1000 and no key error
                application = applications[application_index]
                try:
                    if query_results[application_id][0] > 1000:
                        application.integration_live = IntegrationLive.TRUE
                    else:
                        application.integration_live = IntegrationLive.FALSE
                    application.integration_dau = query_results[application_id][1]
                    application.integration_mau = query_results[application_id][2]
                except KeyError:
                    # No results for this application
                    application.integration_live = IntegrationLive.FALSE
                    application.integration_dau = 0
                    application.integration_mau = 0
                    application.save()
        else:
            logging.error("Unknown table name for host {http_host}.".format(http_host=environ['HTTP_HOST']))
