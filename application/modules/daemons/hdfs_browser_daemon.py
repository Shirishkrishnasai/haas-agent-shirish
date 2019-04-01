import requests, multiprocessing, json, sys, os
from application.common.loggerfile import my_logger
from apscheduler.schedulers.background import BackgroundScheduler
import time
from application.configfile import agentinfo_path, server_url
from application.modules.workers.hdfs_count_worker import hdfsCountworker
from application.modules.workers.hdfs_delete_worker import hdfsDeleteworker
from application.modules.workers.hdfs_fsck_worker import hdfsFSCKworker
from application.modules.workers.hdfs_list_worker import hdfsListworker
from application.modules.workers.hdfs_mkdir_worker import hdfsMkdirworker
from application.modules.workers.hdfs_head_worker import hdfsHeadworker
from application.modules.workers.hdfs_tail_worker import hdfsTailworker
from application.modules.workers.hdfs_move_worker import hdfsMoveworker
from application.modules.workers.hdfs_file_upload_worker import hdfsFileuploadworker
from application.modules.workers.hdfs_file_download_worker import hdfsFiledownloadworker
def hdfsBrowserDaemon():
    try:
        agent_info = open(agentinfo_path, "r")
        content = agent_info.read()
        data_req = json.loads(content, 'utf-8')
        agent_id = str(data_req['agent_id'])

        role = str(data_req['role'])
        if role == 'namenode':

            url = server_url + "api/hdfs/request/"+agent_id 
	    print url,"urlllllllllllll"
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            api_response = requests.get(url, headers=headers).json()
	    print api_response
            if api_response == 000:
                print "no messages for now"
                pass
            else:
                for each_command in api_response['message']:
		    path = each_command['hdfs_parameters']
                    command = each_command['command_type']
                    if command == 'fsck':
                        fsck_process = multiprocessing.Process(target=hdfsFSCKworker,
                                                               args=([path, each_command['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'count':

                        fsck_process = multiprocessing.Process(target=hdfsListworker,
                                                               args=([path, each_command['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'mkdir':
                        fsck_process = multiprocessing.Process(target=hdfsMkdirworker,
                                                               args=([path, each_command['request_id'],each_command['directory_name']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'list':
                        fsck_process = multiprocessing.Process(target=hdfsListworker,
                                                               args=([path, each_command['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'text':
                         fsck_process = multiprocessing.Process(target=hdfsHeadworker,
                                                                args=([path, each_command['request_id']]))
                         fsck_process.start()
                         fsck_process.join()
                    elif command == 'tail':
                         fsck_process = multiprocessing.Process(target=hdfsTailworker,
                                                                args=([path, each_command['request_id']]))
                         fsck_process.start()
                         fsck_process.join()
                    elif command == 'upload':
                         fsck_process = multiprocessing.Process(target=hdfsFileuploadworker,
                                                                args=([each_command['input_path'],path, each_command['request_id']]))
                         fsck_process.start()
                         fsck_process.join()
                    elif command == 'download':
                         fsck_process = multiprocessing.Process(target=hdfsFiledownloadworker,
                                                                args=([path, each_command['request_id']]))
                         fsck_process.start()
                         fsck_process.join()
                    elif command == 'mv':
                         fsck_process = multiprocessing.Process(target=hdfsMoveworker,
                                                                args=([path,each_command['output_path'] ,each_command['request_id']]))
                         fsck_process.start()
                         fsck_process.join()
                    elif command == 'remove':
                        fsck_process = multiprocessing.Process(target=hdfsDeleteworker,
                                                               args=([path, each_command['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
        else:
            print "this node role is not namenode"
            pass
    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
	time.sleep(10)
def hdfsBrowserDaemonScheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(hdfsBrowserDaemon, 'cron', second='*/30')
    scheduler.start()
    pass













