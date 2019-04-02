
'''
Author - shirish
modified - 19-03-2019
'''

import subprocess
from application.configfile import server_url
import requests,sys,os
import json
from application.common.loggerfile import my_logger
import commands

def hdfsTailworker(path,request_id):
    try:
        # change the tatic path
        args="/opt/agent/application/modules/workers/hdfs_tail.sh %s" %path
	tail_command = "hadoop fs -tail -f " + path
	args_list=["hadoop", "fs", "-tail", path]
	proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    	(out_put, errors) = proc.communicate()
	output={"output":{"message":str(out_put)},"request_id":request_id}
        url = server_url + 'api/upload'
	taillist=[]
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(output), headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        my_logger.info('hdfsTailworker finally block')


