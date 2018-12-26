import requests
import json

from datetime import datetime
from application.common.loggerfile import my_logger

from application.configfile import hive_connection, kafka_server_url, file_upload_url

from application.common.hive import HiveQuery
import time
from kafka import KafkaProducer
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblHiveQueryStatus




def hiveResultQueryWorker(query_database,hive_query,hive_request_id,customer_id,cluster_id):

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
    #try:
        print "in hive  result query worker"
        hive_query_decode = hive_query.decode('base64', 'strict')

        hiveClient = HiveQuery(hive_connection, 10000, query_database)

        db_session = scoped_session(session_factory)
        hive_result_data = {}
        hive_result = hiveClient.runQuery(hive_query_decode)
        print hive_result

        if type(hive_result) == dict:
            if hive_result['status'] == 2:
                hive_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                 var_query_status='RUNNING',
                                                 ts_status_datetime=datetime.now(),
                                                 bool_flag=0)
                db_session.add(hive_status)
                db_session.commit()
                string_tuple_list = json.dumps([list(map(str, eachTuple)) for eachTuple in hive_result['output']])
                print string_tuple_list,type(string_tuple_list),"huuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu"

                #output_string = {}
                #output_string[str(hive_result['description'][0][0])] = string_tuple_list



                hive_result_data['output'] = json.dumps(string_tuple_list).encode('base64','strict')
                hive_result_data['message'] = "query executed successfully"

            elif hive_result['status'] == 1 or hive_result['status'] == 7:
                print hive_result, type(hive_result), "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj"
                query_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                  var_query_status=status_dict[hive_result],
                                                  ts_status_datetime=datetime.now(),
                                                  bool_flag=0)
                db_session.add(query_status)
                db_session.commit()
                hive_result_data['message'] = "query is either running or queued ...you should wait"
            else:
                query_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                  var_query_status=status_dict[hive_result],
                                                  ts_status_datetime=datetime.now(),
                                                  bool_flag=0)
                db_session.add(query_status)
                db_session.commit()
                hive_result_data['message'] = str(status_dict[hive_result['status']])
        else:
            hive_result_data['message'] = "an error occured...submit the query again"

        hive_result_data['hive_request_id'] = str(hive_request_id)
        producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
        kafka_topic = "hivequeryresult_" + customer_id + "_" + cluster_id
        kafkatopic = kafka_topic.decode('utf-8')
        producer.send(kafkatopic, str(hive_result_data))
        producer.flush()
        print hive_result_data
        print "produced"
