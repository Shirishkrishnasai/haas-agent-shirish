import logging
import requests, sys, os
from application.common.load_config import loadconfig
from sqlalchemy.orm import scoped_session
from application.models.models import TblAgentWorkerTaskMapping
logging.basicConfig()
from apscheduler.schedulers.background import BackgroundScheduler
from application import sqlite_string, session_factory
from application.configfile import server_url,agentinfo_path
from application.common.loggerfile import my_logger

def agentdaemon():
    try:
        #getting tasks from hgmanager
        session = scoped_session(session_factory)
        # getting agentid from cloudinnit file
        agent_id, customer_id, cluster_id = loadconfig()
        # calling hgmanager for its tasks
        url=server_url+'hgmanager/'+agent_id
        r = requests.get(url)
        req_data = r.json()
        if req_data == 'null' :
            pass
        # posting data in tbl_agent_worker_task_mapping
        else :
            for data in req_data:
                    task_id = str(data['task_id'])
                    payload_id=str(data['payload_id'])
                    worker_version_path=str(data['worker_path'])
                    worker_version=str(data['worker_version'])
                    task_type=str(data['task_type_id'])

                    data=TblAgentWorkerTaskMapping(uid_task_id=task_id,
                                               lng_task_type_id=task_type,
                                               txt_payload_id=payload_id,
                                               var_agent_worker_file_name=worker_version,
                                               txt_path=worker_version_path,
                                               var_task_status="INITIALISED")
                    session.add(data)
                    session.commit()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        session.close()
def agentdaemonscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(agentdaemon,'cron',second='*/5' )
    scheduler.start()
