'''
Author - shirish
modified - 19-03-2019
'''

import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger
from flask import jsonify

def hdfsMkdirworker(path, dirname):
    try:
        url = "http://localhost:50070/webhdfs/v1" + path + "?op=LISTSTATUS"
        response = requests.get(url)
        result= json.loads(response.text)
        file_list = result["FileStatuses"]["FileStatus"]
        req_data = []
        for indivdual_file in file_list:
            if indivdual_file['type'] == 'DIRECTORY':
                req_data.append(str(file['pathSuffix']))
        if str(dirname) in req_data:
            url = server_url + 'api/upload'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            output_dict = {"message":"already exists"}
            requests.post(url, data=json.dumps(output_dict), headers=headers)
        else:
            url = "http://localhost:50070/webhdfs/v1" + path + "/" + dirname + "?user.name=hadoop&op=MKDIRS"
            response1 = requests.put(url)
            result1 = json.loads(response1.text)
            message = result1['boolean']
            if message == True:
                url = server_url + 'api/upload'
                headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
                output_dict = {"message": "success"}
                requests.post(url, data=json.dumps(output_dict), headers=headers)
            else:
                url = server_url + 'api/upload'
                headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
                output_dict = {"message": "failed"}
                requests.post(url, data=json.dumps(output_dict), headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        my_logger.info('hdfsMkdirworker finally block')