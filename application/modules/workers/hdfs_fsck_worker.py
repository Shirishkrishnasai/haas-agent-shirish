import requests,os,sys
import json
from datetime import datetime
from application.configfile import hive_connection, server_url
from application.common.loggerfile import my_logger
from application.common.hive import HiveQuery
from sqlalchemy.orm import scoped_session
from application import session_factory


def hdfsFSCKworker(directory_path,request_id):
    namenodeip = '192.168.100.182'

    response = requests.get(url="http://" + namenodeip + ":50070/webhdfs/v1/" + directory_path + "?op=GETFILESTATUS")
    result = response.json()

    res_dict = {}
    output_dict = {}
    for keys,values in result.items():
        print values
        res_dict['blockSize'] = str(values['blockSize'])
        res_dict['accessTime'] = str(values['accessTime'])
        res_dict['modificationTime'] = str(values['modificationTime'])
        res_dict['replication'] = str(values['replication'])
        res_dict['length'] = str(values['length'])
        res_dict['fileId'] = str(values['fileId'])
    output_dict['output'] = res_dict
    output_dict['request_id'] = request_id
    print output_dict
    # result = response.text
    # result = json.loads(result)
    # print result,type(result)

    url = server_url + ''
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    requests.post(url, data=json.dumps(output_dict), headers=headers)