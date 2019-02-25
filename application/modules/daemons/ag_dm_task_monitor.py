import json
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from application.common.load_config import loadconfig
from sqlalchemy.orm import scoped_session
logging.basicConfig()
import sqlite3
from application import session_factory
from application.configfile import  server_url, hgmonitor_connection
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus
import sys

def agentmonitordaemon():
    # getting status of worker and  uppdating hat information to hg monitor

        try:
            db_session = scoped_session(session_factory)
            agent_id, customer_id, cluster_id = loadconfig()
            print agent_id, customer_id, cluster_id
            select_task_status = db_session.query(TblAgentTaskStatus.uid_task_id,
                                                  TblAgentTaskStatus.var_task_status,
                                                  TblAgentTaskStatus.bool_flag).filter(TblAgentTaskStatus.bool_flag==0).all()
            print select_task_status, "......................in agent task monitor.py"
            my_logger.info("agent task status daemon fetched tasks")
            for each_task in select_task_status:
                print each_task, ".............in agent task monitor.py"
                my_logger.info("in for loop")
                my_logger.info(each_task)
                task_data = {}
                my_logger.info("checked for bool_flag=0 in db")
                task_data['task_id'] = each_task[0]
                task_data['status'] = each_task[1]

                task_status_data = {}
                task_status_data['customer_id'] = customer_id
                task_status_data['cluster_id'] = cluster_id
                task_status_data['agent_id'] = agent_id
                payload_data = {}
                payload_data['task_id'] = str(each_task[0])
                payload_data['status'] = str(each_task[1])
                task_status_data['payload'] = payload_data
                print task_status_data, "..........................................................in agent task monitor.py"
                url=server_url+hgmonitor_connection
                print url, "now this api for status posting................."

                headers={'content-type':'application/json','Accept':'text/plain'}
                value=requests.post(url,data=json.dumps(task_status_data),headers=headers)
                status= value.json()
                print status
                # calu = requests.response(value)
                # print calu
                print "agent monitor daemon posted all the status to serverrrrrrrrr ..."
                if status== "sucess" :
                    print "in if"
                    update_task_flag_query = db_session.query(TblAgentTaskStatus).filter(
                                TblAgentTaskStatus.uid_task_id == each_task[0])
                    update_task_flag_query.update({"bool_flag": 1})
                    db_session.commit()

                print 'task update to hgmonitor successfull'


        except sqlite3.Error as er:
            my_logger.info(er)
            my_logger.info(sys.exc_info()[0])
        except Exception as e:
            print  "Some this went wrong", sys.exc_info()[0]
            my_logger.info(e)
            my_logger.info(sys.exc_info()[0])
        finally:
            db_session.close()


def agentmonitorscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(agentmonitordaemon, 'cron', second='*/9')
    scheduler.start()
