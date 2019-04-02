'''
Author - shirish
modified - 19-03-2019
'''

import subprocess
from application.configfile import server_url
import requests,sys,os
import json
import uuid
from application.common.loggerfile import my_logger

def hdfsFiledownloadworker(source_path,request_id):
    try:
        # change the static path
	filename=uuid.uuid1()
        args="/opt/agent/application/modules/workers/hdfs_file_download.sh %s %s" %(source_path,filename)
        print args,'argsssssssss'
        out_put = subprocess.call(args, shell = True)
        print out_put,'lol'
        if out_put == 0:
            output = {"output":{"message":str(filename)},"request_id":request_id}

            url = server_url + 'api/upload'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
	    print output,type(output)
            requests.post(url, data=json.dumps(output), headers=headers)

        else:
            output = {"output":"not downloaded","request_id":request_id}

            url = server_url + 'api/upload'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(url, data=output, headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        my_logger.info('hdfsFiledownloadworker finally block')
