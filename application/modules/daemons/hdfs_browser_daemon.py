import requests, multiprocessing, json, sys, os
from application.common.loggerfile import my_logger
from apscheduler.schedulers.background import BackgroundScheduler

from application.configfile import agentinfo_path, server_url
from application.modules.workers.hdfs_count_worker import hdfsCountworker
from application.modules.workers.hdfs_delete_worker import hdfsDeleteworker
from application.modules.workers.hdfs_fsck_worker import hdfsFSCKworker
from application.modules.workers.hdfs_list_worker import hdfsListworker
from application.modules.workers.hdfs_mkdir_worker import hdfsMkdirworker

def hdfsBrowserDaemon():
    try:
        agent_info = open(agentinfo_path, "r")
        content = agent_info.read()
        data_req = json.loads(content, 'utf-8')
        agent_id = str(data_req['agent_id'])
        role = str(data_req['role'])
        if role == 'namenode':
            url = server_url + "hdfs/" + agent_id
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            api_response = requests.get(url, headers=headers).json()
            path = api_response['job_parameters']
            if api_response == 000:
                pass
            else:
                for each_command in api_response:

                    command = each_command['command_type']
                    if command == 'fsck':
                        fsck_process = multiprocessing.Process(target=hdfsFSCKworker,
                                                               args=([path, api_response['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'count':

                        fsck_process = multiprocessing.Process(target=hdfsListworker,
                                                               args=([path, api_response['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'mkdir':
                        fsck_process = multiprocessing.Process(target=hdfsMkdirworker,
                                                               args=([path, api_response['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    elif command == 'list':
                        fsck_process = multiprocessing.Process(target=hdfsListworker,
                                                               args=([path, api_response['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
                    # elif command == 'text':
                    #     fsck_process = multiprocessing.Process(target=fsckworker,
                    #                                            args=([path, api_response['request_id']]))
                    #     fsck_process.start()
                    #     fsck_process.join()
                    # elif command == 'tail':
                    #     fsck_process = multiprocessing.Process(target=fsckworker,
                    #                                            args=([path, api_response['request_id']]))
                    #     fsck_process.start()
                    #     fsck_process.join()
                    # elif command == 'upload':
                    #     fsck_process = multiprocessing.Process(target=fsckworker,
                    #                                            args=([path, api_response['request_id']]))
                    #     fsck_process.start()
                    #     fsck_process.join()
                    # elif command == 'download':
                    #     fsck_process = multiprocessing.Process(target=fsckworker,
                    #                                            args=([path, api_response['request_id']]))
                    #     fsck_process.start()
                    #     fsck_process.join()
                    # elif command == 'mv':
                    #     fsck_process = multiprocessing.Process(target=fsckworker,
                    #                                            args=([path, api_response['request_id']]))
                    #     fsck_process.start()
                    #     fsck_process.join()
                    elif command == 'remove':
                        fsck_process = multiprocessing.Process(target=hdfsDeleteworker,
                                                               args=([path, api_response['request_id']]))
                        fsck_process.start()
                        fsck_process.join()
        else:
            pass
    except Exception as e:

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
        #time.sleep(10)

def hdfsBrowserDaemonScheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(hdfsBrowserDaemon, 'cron', second='*/5')
    scheduler.start()
    pass













