

import requests,sys,os
import json,ast
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsCountworker(directory_path,request_id):

    try:
        response = requests.get(url="http://localhost:50070/webhdfs/v1/" + directory_path + "?op=GETCONTENTSUMMARY")
        result = response.json()
        result_dict = ast.literal_eval(json.dumps(result))
        output_dict = {}
        output_dict['output'] = result_dict['ContentSummary']
        output_dict['request_id'] = request_id
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(output_dict), headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
