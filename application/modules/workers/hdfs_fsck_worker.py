'''
Author - shirish
modified - 19-03-2019
'''

import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsFSCKworker(directory_path,request_id):
#    try:
	print directory_path
	url="http://52.172.55.17:50070/webhdfs/v1/" + str(directory_path) + "?op=GETFILESTATUS"
	print url,"urllllllllllllllllllllllllllllllllllll"
        response = requests.get(url)
        result = response.json()
	print result,"resultttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt"
        res_dict = {}
        output_dict = {}
        for keys,values in result.items():
	    print keys,values,'workerrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr'
            res_dict['blockSize'] = str(values['blockSize'])
            res_dict['accessTime'] = values['accessTime']
            res_dict['modificationTime'] = values['modificationTime']
            res_dict['replication'] = str(values['replication'])
            res_dict['length'] = str(values['length'])
            res_dict['fileId'] = str(values['fileId'])
        output_dict['output'] = res_dict
        output_dict['request_id'] = request_id
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(output_dict), headers=headers)
 #   except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
  #  finally:
        my_logger.info('hdfsFSCKworker finally block')
