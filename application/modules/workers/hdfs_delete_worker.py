import requests,os,sys
import json,ast
from datetime import datetime
from application.configfile import hive_connection, server_url
from application.common.loggerfile import my_logger
from application.common.hive import HiveQuery
from sqlalchemy.orm import scoped_session
from application import session_factory


def hdfsDeleteworker(file_path,request_id):
    namenodeip = '192.168.100.182'

    response = requests.delete(url="http://" + namenodeip + ":50070/webhdfs/v1/" + file_path + "?op=DELETE")
    # result = response.text
    result = response.json()
    key = result.keys()[0]
    print key
    result[str(key)] = result.pop(result.keys()[0])
    result['request_id'] = request_id
    # result = json.loads(result)
    print result

    url = server_url + 'api/upload'
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    requests.post(url, data=json.dumps(result), headers=headers)