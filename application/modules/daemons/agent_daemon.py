import logging

import requests
from application.common.load_config import loadconfig
from sqlalchemy.orm import scoped_session
from application.models.models import TblAgentWorkerTaskMapping
logging.basicConfig()
from apscheduler.schedulers.background import BackgroundScheduler
import urllib2
import json
import sqlite3
from application import sqlite_string, session_factory
from application.configfile import server_url,agentinfo_path
def agentdaemon():
    #getting tasks from hgmanager
    session = scoped_session(session_factory)
# getting agentid from cloudinnit file
    agent_id, customer_id, cluster_id = loadconfig()
    print "in agent daemon program"
# calling hgmanager for its tasks
    url=server_url+'hgmanager/'+agent_id
    print url, " this agent daemon just called the hgmanager apiiiiiii"
    r = requests.get(url)
    req_data = r.json()
    print (req_data),type(req_data)
# posting data in tbl_agent_worker_task_mapping
    for data in req_data:
            print data
            task_id = str(data['task_id'])
            print task_id
            payload_id=str(data['payload_id'])
            worker_version_path=str(data['worker_path'])
            worker_version=str(data['worker_version'])
            task_type=str(data['task_type_id'])

            data=TblAgentWorkerTaskMapping(uid_task_id=task_id,
                                       lng_task_type_id=task_type,
                                       txt_payload_id=payload_id,
                                       var_agent_worker_file_name=worker_version,
                                       txt_path=worker_version_path,
                                       var_task_status="initialised")
            session.add(data)
            session.commit()
            session.close()
            print  "agent daemon committed to database. will run in next minute........................meanwhile check the table"
def agentdaemonscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(agentdaemon,'cron',minute='*/1' )
    scheduler.start()