import json
import os
import sys

from application.common.hive import HiveQuery
from application.common.loggerfile import my_logger
from application.configfile import  kafka_server_url, hive_connection
from kafka import KafkaConsumer
from kafka import KafkaProducer
from application.common.load_config import loadconfig
import time

def hiveDatabaseQueryConsumer():
    my_logger.info('in hive query consumer')
    agent_id, customer_id, cluster_id = loadconfig()
    consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id="hivedatabasequery"+str(agent_id))
    consumer.subscribe(pattern='hivedatabasequery*')
    while True:
        try:
            message = consumer.poll(timeout_ms=1000, max_records=1)
            if message != {}:
                topicMesages = message.values()

                for messageValues in topicMesages[0]:

                    my_logger.info("in first for loop")
                    consumer_data = message.value
                    # my_logger.info("..........................",consumer_data
                    data = consumer_data.replace("'", '"')
                    query_data = json.loads(data)
                    my_logger.info('hellllllllllloooooooooooooooooo')
                    my_logger.info(query_data)
                    hiveClient = HiveQuery(hive_connection, 10000, 'default')
                    my_logger.info("hhhhhhhhhhhhhhhhhhheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                    explain_result = hiveClient.runQuery("show databases")
                    my_logger.info(explain_result)
                    my_logger.info(type(explain_result))
                    names_list = []
                    database_result = {}
                    for value in explain_result['output']:
                        my_logger.info(str(value[0]))
                        names_list.append(str(value[0]))
                    my_logger.info(names_list)
                    key = "database_names"
                    database_result[key] = names_list
                    my_logger.info(database_result)
                    producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
                    kafkatopic = "hivedatabaseresult_" + customer_id + "_" + cluster_id
                    kafkatopic = kafkatopic.decode('utf-8')
                    producer.send(kafkatopic, str(database_result))
                    producer.flush()
                    my_logger.info("produced")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
        time.sleep(2)