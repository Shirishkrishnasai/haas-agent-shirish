'''
Author - shirish
modified - 19-03-2019
'''

import requests, sys, os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger


def hdfsDeleteworker(file_path, request_id):
    try:
        response = requests.delete(url="http://localhost:50070/webhdfs/v1/" + file_path + "?op=DELETE")
        result = response.json()
        key = result.keys()[0]
        result1 = {}
        # result['output'] = result.pop(result.keys()[0])
        result1['output'] = {"message": "file removed"}
        result1['request_id'] = request_id
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(result1), headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
