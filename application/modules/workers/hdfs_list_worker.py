import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsListworker(path,request_id):
    try:
        file_command_list = ['fsck','count','remove','tail','text','upload','download','mv']
        directory_command_list = ['list','fsck','count','mkdir']
        response = requests.get(url="http://localhost:50070/webhdfs/v1/" + path + "?op=LISTSTATUS")
        result = response.json()
        result_list = []
        output_dict = {}
        for dicts in result['FileStatuses']['FileStatus']:
            res_dict = {}
            if dicts['type'] == str('FILE'):
                res_dict[str(dicts['pathSuffix'])] = file_command_list
                res_dict['type'] = str(dicts['type'])
                result_list.append(res_dict)
            if dicts['type'] == str('DIRECTORY'):
                res_dict[str(dicts['pathSuffix'])] = directory_command_list
                res_dict['type'] = str(dicts['type'])
                result_list.append(res_dict)
        output_dict['output'] = result_list
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
    finally:
        my_logger.info('hdfsListworker finally block')