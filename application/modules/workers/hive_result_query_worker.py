import requests
import json

from datetime import datetime

from application.configfile import hive_connection, server_url

from application.common.hive import HiveQuery
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
    try:
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

        url = server_url + 'hivequeryoutput'
        data = json.dumps(hive_result_data)
        print data
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=data, headers=headers)
        print 'done'

    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        db_session.close()