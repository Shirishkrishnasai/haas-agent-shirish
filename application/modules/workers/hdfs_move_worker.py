import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsMoveworker(file_path,destination_path,request_id):
    try:
        response = requests.put(url="http://localhost:50070/webhdfs/v1/" + file_path + "?op=RENAME&destination="+destination_path)
        result = response.json()
        key = result.keys()[0]
        result[str(key)] = result.pop(result.keys()[0])
        result['request_id'] = request_id
        print result
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(result), headers=headers)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        my_logger.info('hdfsMoveworker finally block')
