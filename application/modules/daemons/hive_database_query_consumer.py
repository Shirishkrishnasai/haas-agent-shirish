import json
import os
import sys

from application.common.hive import HiveQuery
from application.common.loggerfile import my_logger
from application.configfile import agentinfo_path, kafka_server_url, hive_connection
from kafka import KafkaConsumer
from kafka import KafkaProducer
from application.common.load_config import loadconfig
import time

def hiveDatabaseQueryConsumer():
    my_logger.debug('in hive query consumer')
    agent_id, customer_id, cluster_id = loadconfig()
    consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
    consumer.subscribe(pattern='hivedatabasequery*')
    while True:
        try:
            message = consumer.poll(timeout_ms=1000, max_records=1)
            if message != {}:
                topicMesages = message.values()

                for messageValues in topicMesages[0]:

                    print "in first for loop"
                    consumer_data = message.value
                    # print "..........................",consumer_data
                    data = consumer_data.replace("'", '"')
                    query_data = json.loads(data)
                    print 'hellllllllllloooooooooooooooooo'
                    print query_data
                    hiveClient = HiveQuery(hive_connection, 10000, 'default')
                    print "hhhhhhhhhhhhhhhhhhheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    explain_result = hiveClient.runQuery("show databases")
                    print 'starrrrrrrrrrrrrrttttttttttttt', explain_result
                    print type(explain_result)
                    names_list = []
                    database_result = {}
                    for value in explain_result['output']:
                        print str(value[0])
                        names_list.append(str(value[0]))
                    print names_list
                    key = "database_names"
                    database_result[key] = names_list
                    print database_result
                    producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
                    kafkatopic = "hivedatabaseresult_" + customer_id + "_" + cluster_id
                    kafkatopic = kafkatopic.decode('utf-8')
                    producer.send(kafkatopic, str(database_result))
                    producer.flush()
                    print "produced"
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
        time.sleep(2)