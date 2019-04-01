'''
Author - shirish
modified - 19-03-2019
'''

import requests,sys,os
import json
from application.configfile import server_url
from application.common.loggerfile import my_logger

def hdfsListworker(path,request_id):
#    try:
        file_command_list = ['fsck','count','remove','tail','text','upload','download','mv']
        directory_command_list = ['list','fsck','count','mkdir']
	print path
        response = requests.get(url="http://localhost:50070/webhdfs/v1/" + path + "?op=LISTSTATUS")
        result = response.json()
	print result
        result_list = []
        output_dict = {}
        for dicts in result['FileStatuses']['FileStatus']:
            res_dict = {}
            if dicts['type'] == str('FILE'):
                res_dict['file_name'] = str(dicts['pathSuffix'])
                res_dict['options'] = file_command_list
                res_dict['type'] = str(dicts['type'])
                res_dict['group'] = str(dicts['group'])
                res_dict['permission'] = str(dicts['permission'])
                res_dict['block_size'] = str(dicts['blockSize'])
                res_dict['replication'] = str(dicts['replication'])
                res_dict['length'] = str(dicts['length'])
                res_dict['owner'] = str(dicts['owner'])
		res_dict['path'] = str("/"+path)
		if res_dict['path'] == "/":
			pass
		else:
			path_list=path.split("/")
			res_dict['previous_path']="/"+"/".join(path_list[0:-2])
                result_list.append(res_dict)
            if dicts['type'] == str('DIRECTORY'):
                res_dict['directory_name'] = str(dicts['pathSuffix'])
                res_dict['options'] = directory_command_list
                res_dict['type'] = str(dicts['type'])
                res_dict['group'] = str(dicts['group'])
                res_dict['permission'] = str(dicts['permission'])
                res_dict['block_size'] = str(dicts['blockSize'])
                res_dict['replication'] = str(dicts['replication'])
                res_dict['length'] = str(dicts['length'])
                res_dict['owner'] = str(dicts['owner'])
		res_dict['path'] = str("/"+path)
                if res_dict['path'] == "/":
                        pass
                else:
                        path_list=path.split("/")
                        res_dict['previous_path']="/"+"/".join(path_list[0:-2])

                result_list.append(res_dict)
        output_dict['output'] = result_list
        output_dict['request_id'] = request_id
        url = server_url + 'api/upload'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(output_dict), headers=headers)

#    except Exception as e:
 #       exc_type, exc_obj, exc_tb = sys.exc_info()
  #      fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

   #     my_logger.error(exc_type)
    #    my_logger.error(fname)
     #   my_logger.error(exc_tb.tb_lineno)
   # finally:
        my_logger.info('hdfsListworker finally block')


