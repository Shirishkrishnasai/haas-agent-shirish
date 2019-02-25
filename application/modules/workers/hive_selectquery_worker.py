import requests
import json
import os
from datetime import datetime

from application.configfile import hive_connection, kafka_server_url, file_upload_url,server_url
from application.common.hive import HiveQuery
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblHiveQueryStatus

def hiveSelectQueryWorker(output_type,query_database,explain_query,hive_query,hive_request_id,customer_id,cluster_id):
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
        print "in hive select query worker"
        hiveClient = HiveQuery(hive_connection, 10000, query_database)
        db_session = scoped_session(session_factory)


        explain_query_decode = explain_query.decode('base64', 'strict')

        hive_query_decode = hive_query.decode('base64', 'strict')

        explain_result = hiveClient.runQuery(explain_query_decode)

        hive_result_data = {}
        if type(explain_result) == dict:

            if explain_result['status'] == 2:

                explain_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                    var_query_status='RUNNING',
                                                    ts_status_datetime=datetime.now(),
                                                    bool_flag=0)
                db_session.add(explain_status)
                db_session.commit()

                hive_result = hiveClient.runNoResultQuery(hive_query_decode)
                print hive_result, type(hive_result)
                if hive_result == 2:
                    explain_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                        var_query_status='FINISHED',
                                                        ts_status_datetime=datetime.now(),
                                                        bool_flag=0)
                    db_session.add(explain_status)
                    db_session.commit()

                    #mnt_file = os.system('hadoop fs -copyToLocal /hive_request_id /mnt/MyAzureFileShare/')
                    # mnt_file = os.system('hdfs dfs -cat /'+hive_request_id+'/000000_0 >> /opt/mnt/MyAzureFileShare/'+hive_request_id)
                    mnt_file = os.system("hdfs dfs -cat /" + hive_request_id + "/000000_0 | sed -e  's/\x01/,/g'> /opt/mnt/azurefileshare/hive/" + hive_request_id)
                    print mnt_file
                    print "ok now check the mount directory...u r doneeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

                    hive_result_data['message'] = "query executed successfully,the result is being uploaded."

                elif hive_result == 1 or hive_result == 7:
                    print hive_result, type(hive_result)
                    query_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                      var_query_status=status_dict[hive_result],
                                                      ts_status_datetime=datetime.now(),
                                                      bool_flag=0)
                    db_session.add(query_status)
                    db_session.commit()



                    hive_result_data['message'] = "query is either running or queued ...you should wait"

                elif type(hive_result) == int:
                    explain_status = TblHiveQueryStatus(uid_hive_request_id=hive_request_id,
                                                        var_query_status=status_dict[hive_result],
                                                        ts_status_datetime=datetime.now(),
                                                        bool_flag=0)
                    db_session.add(explain_status)
                    db_session.commit()

                    hive_result_data['message'] = str(status_dict[hive_result])

                else:
                    hive_result_data['message'] = "an error occured...submit the query again"


        else:
            hive_result_data['message'] = "invalid query"

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