from application.configfile import hive_connection
from application.common.hive import HiveQuery
import sys
#print sys.path
#sys.path.append('./application/common/')
from application.common.hive import HiveQuery
#print sys.path
import time
from pyhive import hive
from TCLIService.ttypes import TOperationState
from kafka import KafkaProducer


#
def hiveSelectQueryResult(output_type,query_database,explain_query,hive_query,hive_request_id,customer_id,cluster_id):
    print query_database ,"query"
    print explain_query,"explain"
    explain_query_decode = explain_query.decode('base64', 'strict')
    print explain_query
    hive_query_decode = hive_query.decode('base64', 'strict')
    hiveClient = HiveQuery(hive_connection, 10000, query_database)
    #passed_args = sys.argv
    #print passed_args,"lol"
    #query = query_data['database']
    print "in hive wooooooorrrrrrrrrrrkkkkkkeeeeeeeeerrrrrrrrrr"
    #for i in passed_args:
    #    print i
    #print passed_args[0],type(passed_args[0])
    #print passed_args[1],type(passed_args[1])
    #hiveClient = HiveQuery("localhost", 10000, passed_args[1])
    #print "hive connection ok"
    explain_result = hiveClient.runQuery(explain_query_decode)
    print explain_result,"exxxxxxxxxplainnnnn"

    hive_result = hiveClient.runNoResultQuery(hive_query_decode)
    print hive_result, "hivvvvvvvvvvvvvvvvveeeeeeeeeeee"
    status_dict = {
        0: "INITIALIZED",
        1: "RUNNING",
        2: "FINISHED",
        3: "CANCELED",
        4: "CLOSED",
        5: "ERROR",
        6: "UNKNOWN",
        7: "PENDING",
        8: "TIMEDOUT",
    }
    #hive_result = hiveClient.runNoResultQuery('select * from sambar')
    if type(hive_result) == int :
        hive_res = int(hive_result)
        for key,value in status_dict.items():
          if hive_res == int(key):
                print hive_res, "hive_res"
                print value,"lollllll"
                result = value
    elif type(hive_result) == str:
        result = "error"

    hive_result_data = {}
    hive_result_data['result'] = result
    hive_result_data['hive_request_id'] = str(hive_request_id)
    producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
    kafka_topic = "hive_result_query_" + customer_id +"_"+ cluster_id

    kafkatopic = kafka_topic.decode('utf-8')
    producer.send(kafkatopic, str(hive_result_data))
    producer.flush()
