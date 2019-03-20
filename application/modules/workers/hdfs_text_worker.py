'''
Author - shirish
modified - 19-03-2019
'''

import subprocess
import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsTextworker(hdfs_file_path,request_id,number_of_lines):
    try:

        namenode = 'http://192.168.100.182'

        hdfs_api_host = namenode + ':50070'
        rest_suffix = '/webhdfs/v1'
        read_file_operation = '?op=OPEN'

        file_read_url = hdfs_api_host + rest_suffix + hdfs_file_path + read_file_operation
        print file_read_url
        str_content = requests.get(file_read_url).content
        cont = str_content.split('\n')
        output_list = []
        for con in cont[0:number_of_lines]:
            output_tup = []
            output_tup.append(con)
            output_list.append(output_tup)
        print output_list
        output_dict = {}
        output_dict['output'] = output_list
        output_dict['request_id'] = request_id
        print output_dict
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
        my_logger.info('hdfsTextworker finally block')

