import os,sys
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




def _hiveQueryConsumer():
    my_logger.info('in hive query consumer')
    info = open(agentinfo_path, "r")
    content = info.read()
    data_req = json.loads(content, 'utf-8')
    agent_id = str(data_req['agent_id'])
    customer_id = str(data_req['customer_id'])
    cluster_id = str(data_req['cluster_id'])
    print "Connecting to ", kafka_server_url
    consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
    consumer.subscribe(pattern='hivequery*')


    while True:
        try:

            message = consumer.poll(timeout_ms=1000, max_records=1)
            if message != {}:
                topicMesages = message.values()

                for messageValues in topicMesages[0]:
                    try:

                        db_session = scoped_session(session_factory)
                        consumer_data = messageValues.value
                        #print "..........................",consumer_data
                        data = consumer_data.replace("'", '"')
                        query_data = json.loads(data)
                        #print 'hellllllllllloooooooooooooooooo'
                        print query_data
                        if agent_id == query_data['agent_id']:
                            my_logger.info("agent_id verification done.....and this is true agent")
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
                    except Exception as e :
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        my_logger.error(exc_type)
                        my_logger.error(fname)
                        my_logger.error(exc_tb.tb_lineno)

            else:
                my_logger.info("hive query consumer received no messages")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
        time.sleep(5)

def hiveQueryConsumer():
    try:
        _hiveQueryConsumer()
    except Exception as e:
        my_logger.info("CAlling itself..,kafkataskconsumer")
        hiveQueryConsumer()
