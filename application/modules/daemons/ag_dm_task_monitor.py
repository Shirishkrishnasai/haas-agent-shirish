import json
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from application.common.load_config import loadconfig
from sqlalchemy.orm import scoped_session

logging.basicConfig()
from application import session_factory
from application.configfile import  server_url
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus
import os, sys


def agentmonitordaemon():
    # getting status of worker and  uppdating hat information to hg monitor
    try:
        taskstatus_db_session = scoped_session(session_factory)
        agent_id, customer_id, cluster_id = loadconfig()
        agent_task_statuses = taskstatus_db_session.query(TblAgentTaskStatus.uid_task_id,
                                                          TblAgentTaskStatus.var_task_status).filter(
            TblAgentTaskStatus.bool_flag == 0).all()
        if agent_task_statuses !=[]:

            print(
                "the following tasks are yet to be give to server ...........there you go ................................")
            print(agent_task_statuses)

            for each_status_row in agent_task_statuses:
                print("now in the for loop of task status ................this is the task")
                print each_status_row
                task_status_post_json = {}
                task_status_post_json['status'] = each_status_row[1]
                task_status_post_json['cluster_id'] = cluster_id
                # post_url = server_url+hgmonitor_connection+each_status_row[0]
                post_url = server_url + "servertaskstatus/" + str(each_status_row[0])
                headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
                response_output = requests.post(post_url, data=json.dumps(task_status_post_json), headers=headers)
                response_json = response_output.json()
                print("this is the response from server after posting")
                print(
                    "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                print response_json
		
                if response_json['message'] == 'success' and response_json['taskid'] == each_status_row[0] and response_json['return_status'] ==each_status_row[1]:
		    status_to_update = taskstatus_db_session.query(TblAgentTaskStatus.var_task_status).filter(TblAgentTaskStatus.uid_task_id == response_json['taskid']).first()
		    if str(status_to_update[0]) == response_json['return_status']:
                    	update_task_flag_query = taskstatus_db_session.query(TblAgentTaskStatus).filter(
                        TblAgentTaskStatus.uid_task_id == each_status_row[0])
                    	update_task_flag_query.update({"bool_flag": 1})
	            	taskstatus_db_session.commit()
                    	taskstatus_db_session.close()
                    	print("committingggggggggggggggg to agent task table done")
		    else:
			pass
		else:
			pass
	
        else:
            print "no task statuses"

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        taskstatus_db_session.close()


def agentmonitorscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(agentmonitordaemon, 'cron', second='*/5')
    scheduler.start()


