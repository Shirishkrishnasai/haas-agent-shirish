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
        print path
        # change the static path
        args="/haas-agent/application/modules/workers/hdfs_head.sh %s" %path
        print args,'argsssssssss'
        out_put = subprocess.check_output(args, shell = True)
        print out_put,type(out_put) ,'lolllllllllll'
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(out_put), headers=headers)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        my_logger.info('hdfsTailworker finally block')