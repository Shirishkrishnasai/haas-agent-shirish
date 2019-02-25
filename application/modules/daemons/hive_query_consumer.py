import json,requests,multiprocessing
from apscheduler.schedulers.background import BackgroundScheduler
import sys, os
from application import session_factory
from application.common.loggerfile import my_logger
from application.configfile import  server_url
from application.modules.workers.hive_noresult_query_worker import hiveNoResultQueryWorker
from application.modules.workers.hive_result_query_worker import hiveResultQueryWorker
from application.modules.workers.hive_selectquery_worker import hiveSelectQueryWorker
from sqlalchemy.orm import scoped_session
from application.common.load_config import loadconfig
def hiveQueryConsumer():
            agent_id,customer_id,cluster_id = loadconfig()
    #while True:
    #try:
            #agent_id = 'c188975e-251b-11e9-8b29-000d3af26ae3'
            url = server_url+"hivequery/"+agent_id
            my_logger.info(url)
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            api_response = requests.get(url, headers = headers).json()
            my_logger.info(json.dumps(api_response))
            my_logger.info('done')
            if api_response == 404:
                my_logger.info("no messages for now")
                pass
            else:

                for query_data in api_response:
                        db_session = scoped_session(session_factory)
                        hive_query = query_data['query_string']
                        query_database = query_data['database']
                        output_type = query_data['output_type']
                        hive_request_id = query_data['hive_request_id']

                        if query_data['output_type'] == 'url':

                            explain_query = query_data['explain']
                            select_query_process = multiprocessing.Process(target=hiveSelectQueryWorker,
                                                                           args=([output_type, query_database,
                                                                                  explain_query, hive_query,
                                                                                  hive_request_id, customer_id,
                                                                                  cluster_id, ]))
                            select_query_process.start()
                            my_logger.info("select query running..........so takes time..........please be patient")
                            select_query_process.join()
                            my_logger.info("select query process done........")

                        elif query_data['output_type'] == 'tuples':
                            result_query_process = multiprocessing.Process(target=hiveResultQueryWorker,
                                                                           args=([query_database, hive_query,
                                                                                  hive_request_id, customer_id,
                                                                                  cluster_id, ]))
                            result_query_process.start()
                            my_logger.info("its a query that has result..........please be patient")
                            result_query_process.join()
                            my_logger.info("hive result query process done........")

                        else:
                            noresult_query_process = multiprocessing.Process(target=hiveNoResultQueryWorker,
                                                                             args=([query_database, hive_query,
                                                                                    hive_request_id, customer_id,
                                                                                    cluster_id, ]))
                            noresult_query_process.start()
                            my_logger.info(
                                "no result for this query...probably a ddl query..........please be patient")
                            noresult_query_process.join()
                            my_logger.info("ddl query process done........")
                        my_logger.info('ok')
                        db_session.close()


    # except Exception as e:
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         my_logger.error(exc_type)
    #         my_logger.error(fname)
    #         my_logger.error(exc_tb.tb_lineno)
    # #time.sleep(10)


def hiveQueryConsumerScheduler():
	scheduler = BackgroundScheduler()
	scheduler.add_job(hiveQueryConsumer,'cron',second='*/5')
	scheduler.start()
	pass
