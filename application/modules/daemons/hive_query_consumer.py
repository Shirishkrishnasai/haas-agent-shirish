import os
import csv
import json
import time
import subprocess
from subprocess import Popen, PIPE, STDOUT
from kafka import KafkaConsumer
from application.common.loggerfile import my_logger
from application.configfile import agentinfo_path, kafka_server_url,hive_connection,hive_output_path
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.common.hive import HiveQuery
from application.models.models import TblHiveQueryStatus
from datetime import datetime
import multiprocessing
#from application.modules.daemons.hive_explainquery_worker import hiveExplain
from application.modules.workers.hive_selectquery_worker import hiveSelectQueryWorker
from application.modules.workers.hive_result_query_worker import hiveResultQueryWorker
from application.modules.workers.hive_noresult_query_worker import hiveNoResultQueryWorker


#haaslib=__import__('haas-libs')




def hiveQueryConsumer():

    while True:
        try:
            my_logger.debug('in hive query consumer')
            info = open(agentinfo_path, "r")
            content = info.read()
            data_req = json.loads(content, 'utf-8')
            agent_id = str(data_req['agent_id'])
            customer_id = str(data_req['customer_id'])
            cluster_id = str(data_req['cluster_id'])

            consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
            consumer.subscribe(pattern='hivequery*')
            try:
                consumer.poll()
                my_logger.debug("Polling consumer...")
                for message in consumer:
                    db_session = scoped_session(session_factory)
                    consumer_data = message.value
                    #print "..........................",consumer_data
                    data = consumer_data.replace("'", '"')
                    query_data = json.loads(data)
                    #print 'hellllllllllloooooooooooooooooo'
                    print query_data
                    if agent_id == query_data['agent_id']:
                        my_logger.debug("agent_id verification done.....and this is true agent")
                        hive_query = query_data['query_string']
                        query_database = query_data['database']
                        output_type = query_data['output_type']
                        hive_request_id = query_data['hive_request_id']


                        #if query_data['output_type'] == 'url' or query_data['output_type'] == 'select':
                        if query_data['output_type'] == 'url':

                            explain_query = query_data['explain']
                            select_query_process = multiprocessing.Process(target=hiveSelectQueryWorker,
                                                                           args=([output_type, query_database, explain_query, hive_query, hive_request_id, customer_id,cluster_id, ]))
                            select_query_process.start()
                            my_logger.info("select query running..........so takes time..........please be patient")
                            select_query_process.join()
                            my_logger.info("select query process done........")

                        elif query_data['output_type'] == 'tuples':
                            result_query_process = multiprocessing.Process(target=hiveResultQueryWorker,
                                                                           args=([query_database, hive_query, hive_request_id, customer_id,cluster_id, ]))
                            result_query_process.start()
                            my_logger.info("its a query that has result..........please be patient")
                            result_query_process.join()
                            my_logger.info("hive result query process done........")
                            #column_names = query_result['description'][0][0]
                            #row_list = query_result['output']

                        else:
                            noresult_query_process = multiprocessing.Process(target=hiveNoResultQueryWorker,
                                                                           args=([query_database, hive_query, hive_request_id, customer_id,cluster_id, ]))
                            noresult_query_process.start()
                            my_logger.info("no result for this query...probably a ddl query..........please be patient")
                            noresult_query_process.join()
                            my_logger.info("ddl query process done........")
                        consumer.commit()
                        print 'ok'
                        db_session.close()

                    else:
                        my_logger.info("agent id is incorrect")

            except Exception as e:
                print e

        except Exception as e:
            print e
