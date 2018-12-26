import logging
logging.basicConfig()
import urllib2
import json
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from application import sqlite_string
from application.configfile import server_url,agentinfo_path,hgmanager_connection
from application.common.loggerfile import my_logger
def agentdaemon():
# getting agentid from cloudinnit file
	try:
		agent_data=open(agentinfo_path,"r")
		contents=agent_data.read()
		agent_data.close()
		data_req=json.loads(contents,'utf-8')
		agent_id=data_req['agent_id']
# calling hgmanager for its tasks
		url=server_url+hgmanager_connection+agent_id
		response=urllib2.urlopen(url)
		frm_hgmanager=response.read()
		tasks_data=json.loads(frm_hgmanager)
		response_data=tasks_data.get("data")
# posting data in tbl_agent_worker_task_mapping
		conn = sqlite3.connect(sqlite_string)
		for data in response_data:
			task_id=data['task_id']
			payload_id=data['payload_id']
			worker_version_path=data['agent_worker_version_path']
			worker_version=data['agent_worker_version']
			task_type=data['task_type_id']
			cur=conn.cursor()
			insert_agent_worker_task="insert into tbl_agent_worker_tasks(uid_task_id,lng_task_type_id,txt_payload_id,var_agent_worker_file_name,txt_path) values('%s','%s','%s','%s','%s')"
			cur.execute(insert_agent_worker_task % (task_id,task_type,payload_id,worker_version,worker_version_path))
			conn.commit()
		return 'agent tasks updated'

	except sqlite3.Error as er:
		my_logger.debug(er)
		return 'sqlite error'
	except Exception:
		return 'data error'
def agentdaemonscheduler():
	scheduler = BackgroundScheduler()
	scheduler.add_job(agentdaemon,'cron',minute='*/1' )
	scheduler.start()

