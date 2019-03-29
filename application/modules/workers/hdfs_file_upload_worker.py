'''
Author - shirish
modified - 19-03-2019
'''

import subprocess
from application.configfile import server_url
import requests,sys,os
import json
from application.common.loggerfile import my_logger

def hdfsFileuploadworker(source_path,destination_path,request_id):
    try:
        #change the static path
        args="/opt/agent/application/modules/workers/hdfs_file_upload.sh %s %s" %(source_path ,destination_path)
        out_put = subprocess.call(args, shell = True)
        if out_put == 0:
            output =  {'output':{'message':'file uploaded'},'request_id':request_id}
            url = server_url + 'api/upload'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(url, data=json.dumps(output), headers=headers)

        else:
            output =  {'output':{'message':'file not uploaded'},'request_id':request_id}
            url = server_url + 'api/upload'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(url, data=json.dumps(output), headers=headers)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
