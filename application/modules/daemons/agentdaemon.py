import logging
logging.basicConfig()
from apscheduler.schedulers.background import BackgroundScheduler
import urllib2
import json
import sqlite3
from application import sqlite_string
from application.configfile import server_url,agentinfo_path
def agentdaemon():
# getting agentid from cloudinnit file 
    file=open(agentinfo_path,"r")
    contents=file.read()
    u=contents.decode('utf-8-sig')
    contents=u.encode('utf-8')
    file.encoding
    file.close()
    data_req=json.loads(contents,'utf-8')
    print data_req
    agentid=data_req['agent_id']
# calling hgmanager for its tasks
    url=server_url+'hgmanager/'+agentid
    print url
    response=urllib2.urlopen(url)
    data=response.read()
    dic=json.loads(data)
    print type(dic)
    req_data=dic.get("data")
    print type(req_data)
# posting data in tbl_agent_worker_task_mapping
    conn = sqlite3.connect(sqlite_string)
    for dat in req_data:
	task_id=dat['task_id']
	payload_id=dat['payload_id']
        worker_version_path=dat['agent_worker_version_path']
        worker_version=dat['agent_worker_version']
        task_type=dat['task_type_id']
        print worker_version,worker_version_path
        cur=conn.cursor()
        cur.execute("insert into tbl_agent_worker_task_mapping (uid_task_id,lng_task_type_id,txt_payload_id,var_agent_worker_file_name,txt_path) values('%s','%s','%s','%s','%s')" % (task_id,task_type,payload_id,worker_version,worker_version_path))
        conn.commit()
def agentdaemonscheduler():
	scheduler = BackgroundScheduler()
	scheduler.add_job(agentdaemon,'cron',minute='*/1' )
	scheduler.start()
