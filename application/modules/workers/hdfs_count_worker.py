import requests,os,sys
import json,ast
from datetime import datetime
from application.configfile import hive_connection, server_url
from application.common.loggerfile import my_logger
from application.common.hive import HiveQuery
from sqlalchemy.orm import scoped_session
from application import session_factory


def hdfsCountworker(directory_path,request_id):
    namenodeip = '192.168.100.182'

    response = requests.get(url="http://" + namenodeip + ":50070/webhdfs/v1/" + directory_path + "?op=GETCONTENTSUMMARY")
    result = response.json()
    # result = json.loads(result)
    result_dict = ast.literal_eval(json.dumps(result))
    output_dict = {}
    output_dict['output'] = result_dict['ContentSummary']
    output_dict['request_id'] = request_id
    print output_dict,type(output_dict)

    url = server_url + 'api/upload'
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    requests.post(url, data=json.dumps(output_dict), headers=headers)