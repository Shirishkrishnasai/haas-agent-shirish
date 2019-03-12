import requests,os,sys
import json
from datetime import datetime
from application.configfile import hive_connection, server_url
from application.common.loggerfile import my_logger
from application.common.hive import HiveQuery
from sqlalchemy.orm import scoped_session
from application import session_factory


def hdfsListworker(path,request_id):
    file_command_list = ['fsck','count','remove','tail','text','upload','download','mv']
    directory_command_list = ['list','fsck','count','mkdir']
    namenodeip = '192.168.100.182'

    response = requests.get(url="http://" + namenodeip + ":50070/webhdfs/v1/" + path + "?op=LISTSTATUS")
    result = response.json()

    # result = response.text
    # result = json.loads(result)
    print result
    result_list = []
    output_dict = {}

    for dicts in result['FileStatuses']['FileStatus']:
        res_dict = {}
        print dicts
        if dicts['type'] == str('FILE'):
            print 'file'
            res_dict[str(dicts['pathSuffix'])] = file_command_list
            res_dict['type'] = str(dicts['type'])
            result_list.append(res_dict)
        if dicts['type'] == str('DIRECTORY'):
            print 'directory'
            res_dict[str(dicts['pathSuffix'])] = directory_command_list
            res_dict['type'] = str(dicts['type'])
            result_list.append(res_dict)
    print result_list
    output_dict['output'] = result_list
    output_dict['request_id'] = request_id
    print output_dict
    url = server_url + 'api/upload'
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    requests.post(url, data=json.dumps(output_dict), headers=headers)