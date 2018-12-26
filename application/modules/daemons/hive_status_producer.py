import datetime
import time
import requests
import json
from kafka import KafkaProducer
import logging
import sys
logging.basicConfig()
import sqlite3
from application.configfile import kafka_server_url, agentinfo_path
from application.common.loggerfile import my_logger
from sqlalchemy.orm import scoped_session
from application import session_factory

from application.models.models import TblHiveQueryStatus


def hiveQueryStatus():
    while True:
        try:

            producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
            info = open(agentinfo_path, "r")
            content = info.read()
            data_req = json.loads(content, 'utf-8')
            customer_id = str(data_req['customer_id'])
            cluster_id = str(data_req['cluster_id'])
            agent_id = str(data_req['agent_id'])



            kafka_topic = "hivequerystatus_" + customer_id + "_" + cluster_id
            my_logger.debug(kafka_topic)
            kafkatopic = kafka_topic.decode('utf-8')
            my_logger.debug(kafkatopic)
            query_status_data = {}
            query_status_data['event_type'] = "query_status"

            query_status_data['customer_id'] = customer_id
            query_status_data['cluster_id'] = cluster_id
            query_status_data['agent_id'] = agent_id
            db_session = scoped_session(session_factory)
            status_data = db_session.query(TblHiveQueryStatus.uid_hive_request_id,
                                           TblHiveQueryStatus.var_query_status,
                                           TblHiveQueryStatus.ts_status_datetime).filter(TblHiveQueryStatus.bool_flag==0).all()
            for each_row in status_data:
                query_status_data['hive_request_id'] = str(each_row[0])
                query_status_data['hive_query_status'] = str(each_row[1])
                query_status_data['status_time'] = str(each_row[2])




                my_logger.debug("print all the data that has to be given from kafka hiveeeee query status producer")
                my_logger.debug(query_status_data)

                producer.send(kafkatopic, str(query_status_data))
                producer.flush()
                my_logger.debug("doneeeeeeee-doneeeeeeeeeeee-londonnnnnnnnnnnnn")
                my_logger.debug("done for agent side hive query status producer to send data through kafka pipeline")

                update_querystatus_flag = db_session.query(TblHiveQueryStatus).filter(
                    TblHiveQueryStatus.uid_hive_request_id == each_row[0])
                update_querystatus_flag.update({"bool_flag": 1})
                db_session.commit()
                db_session.close()

                my_logger.debug("everything doneeeeeeeeeeeeeeeeeeeeeeeeeee")

            producer.close()
        except sqlite3.Error as er:
            my_logger.debug(er)
            my_logger.debug(sys.exc_info()[0])
            #return 'sqlite error'
        except Exception as e:
            print  "Some this went wrong", sys.exc_info()[0]
            my_logger.debug(e)
            my_logger.debug(sys.exc_info()[0])
            #return e.message
        #producer.close()
        time.sleep(20)


def hiveStatusScheduler():
    #scheduler = BackgroundScheduler()
    #scheduler.add_job(agentmonitordaemon, 'cron', minute='*/1')
    #scheduler.start()
    hiveQueryStatus()
