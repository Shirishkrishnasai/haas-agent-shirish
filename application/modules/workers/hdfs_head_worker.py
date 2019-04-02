'''
Author - shirish
modified - 19-03-2019
'''

import subprocess
from application.configfile import server_url
import requests,sys,os
import json
from application.common.loggerfile import my_logger

def hdfsHeadworker(path,request_id):
    try:
        # change the static path
        args="/opt/agent/application/modules/workers/hdfs_head.sh %s" %path
	headlist=[]
        out_put = subprocess.check_output(args, shell = True)
        url = server_url + 'api/upload'
	headlist.append({"message":out_put})
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
	head_data = {"output":headlist,"request_id":str(request_id)}
        requests.post(url, data=json.dumps(head_data), headers=headers)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        my_logger.info('hdfsTailworker finally block')
