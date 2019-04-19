
'''
Author - shirish
modified - 19-03-2019
'''

import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsRenameworker(file_path,destination_path,request_id):
   try:
        filepath_list=file_path.split("/")
        requests.put(url="http://localhost:50070/webhdfs/v1" + file_path + "?op=RENAME&destination="+"/".join(filepath_list[0:-1])+"/"+destination_path)
        output_data={}
        output_data['output'] = {"message":"renamed successfully"}
        output_data['request_id'] = request_id
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(output_data), headers=headers)
   except Exception as e:
       exc_type, exc_obj, exc_tb = sys.exc_info()
       fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
       my_logger.error(exc_type)
       my_logger.error(fname)
       my_logger.error(exc_tb.tb_lineno)
   finally:
        my_logger.info('hdfsMoveworker finally block')