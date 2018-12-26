import sys
import logging
logging.basicConfig()
from apscheduler.schedulers.background import BackgroundScheduler
from application import sqlite_string
import requests
import json
import sqlite3
from application.configfile import server_url, agentinfo_path
def agentmonitordaemon():
# getting status of worker and  uppdating hat information to hg monitor
    conn = sqlite3.connect(sqlite_string)
    cur = conn.cursor()
    statement = "select uid_task_id,var_task_status,bool_flag from tbl_agent_task_status"
    cur.execute(statement)
    tasks=cur.fetchall()
    for task in tasks:
#        print task[2]
        data = {}
        if task[2] == "f":
            data['task_id']=task[0]
            data['status']=task[1]
            url=server_url+'hgmonitor'
            headers={'content-type':'application/json','Accept':'text/plain'}
	    print url,json.dumps(data)
            r=requests.post(url,data=json.dumps(data),headers=headers)
#            print type(data)
#            print data
        statement1="update tbl_agent_task_status set bool_flag='t'where uid_task_id="+"'"+task[0]+"'"
        cur.execute(statement1)
        conn.commit()

def agentmonitorscheduler():
        scheduler = BackgroundScheduler()
        scheduler.add_job(agentmonitordaemon,'cron',minute='*/1' )
        scheduler.start()

