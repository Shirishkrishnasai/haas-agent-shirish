import json
from application.configfile import server_url, agentinfo_path
import datetime
import time
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import os
def hiveStatusWorker():
    info = open(agentinfo_path, "r")
    content = info.read()
    data_req = json.loads(content, 'utf-8')
    customer_id = str(data_req['customer_id'])
    cluster_id = str(data_req['cluster_id'])
    role = str(data_req['role'])
    info.close()
    if role == 'hive' :
        output_hive_server2 = map(str, os.popen('netstat -nlt | grep 10000').readlines()[0].split()[1:])
        output_mysql = map(str, os.popen('systemctl status mysql').readlines())
        vmname = output_mysql[-1].split(" ")
        status_mysql = output_mysql[2].split()[1:]
        date_time = datetime.datetime.now()
        time_value = str(int(round(time.mktime(date_time.timetuple()))) * 1000)
        if output_hive_server2[-1] == 'LISTEN' and status_mysql[1] == '(running)':
            message = "running"
        else:
            message = "dead"
        hive_status_data = {}
        hive_status_data['customer_id'] = customer_id
        hive_status_data['cluster_id'] = cluster_id
        hive_status_data['time_stamp'] = time_value
        payload_data = {}
        payload_data['status'] = message
        payload_data['vm_name'] = vmname[3]
        payload_data['role'] = 'hive'
        hive_status_data['payload'] = payload_data
        url = server_url + "hive_stat"
        print url, "now this api for status posting................."
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        output_response = requests.post(url, data=json.dumps(hive_status_data), headers=headers)
        status = output_response.json()
        print status
    else :
        pass
def hive_work_status_schduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(hiveStatusWorker, 'cron', minute='*/1')
    scheduler.start()