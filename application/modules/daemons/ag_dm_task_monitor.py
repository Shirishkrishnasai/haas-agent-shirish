import datetime
import json
import logging
import sys
import time

import requests
from application.common.load_config import loadconfig
from kafka import KafkaProducer
from sqlalchemy.orm import scoped_session

logging.basicConfig()
import sqlite3
from application import sqlite_string, db, session_factory
from application.configfile import kafka_server_url, agentinfo_path, server_url, hgmonitor_connection
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus
import os, sys

def agentmonitordaemon():
    # getting status of worker and  uppdating hat information to hg monitor

    while True:
        try:
            db_session = scoped_session(session_factory)
            agent_id, customer_id, cluster_id = loadconfig()
            print agent_id, customer_id, cluster_id
            select_task_status = db_session.query(TblAgentTaskStatus.uid_task_id,TblAgentTaskStatus.var_task_status,TblAgentTaskStatus.bool_flag).all()
            print select_task_status, ".............in agent task monitor.py"
            my_logger.info("agent task status daemon fetched tasks")
            for each_task in select_task_status:
                print each_task, ".............in agent task monitor.py"
                my_logger.info("in for loop")
                my_logger.info(each_task)
                task_data = {}
                if each_task[2] == bool(0):
                    my_logger.info("checked for bool_flag=0 in db")
                    task_data['task_id'] = each_task[0]
                    task_data['status'] = each_task[1]
                    date_time = datetime.datetime.now()
                    milliseconds_date = str(int(round(time.mktime(date_time.timetuple()))) * 1000)
                    task_status_data = {}
                    task_status_data['time'] = milliseconds_date
                    task_status_data['customer_id'] = customer_id
                    task_status_data['cluster_id'] = cluster_id
                    task_status_data['agent_id'] = agent_id
                    payload_data = {}
                    payload_data['task_id'] = str(each_task[0])
                    payload_data['status'] = str(each_task[1])
                    payload_data['message'] = "incomplete for now"
                    task_status_data['payload'] = payload_data
                    my_logger.info("print all the data that has to be given from kafka status producer")
                    print task_status_data, ".............in agent task monitor.py"
                    url=server_url+hgmonitor_connection
                    print url
                    headers={'content-type':'application/json','Accept':'text/plain'}
                    requests.post(url,data=json.dumps(task_status_data),headers=headers)
                    update_task_flag_query = db.session.query(TblAgentTaskStatus).filter(
                        TblAgentTaskStatus.uid_task_id == each_task[0])
                    update_task_flag_query.update({"bool_flag": 1})
                    db.session.commit()
                    print 'task update to hgmonitor successfull'
        except sqlite3.Error as er:
            my_logger.info(er)
            my_logger.info(sys.exc_info()[0])
        except Exception as e:
            print  "Some this went wrong", sys.exc_info()[0]
            my_logger.info(e)
            my_logger.info(sys.exc_info()[0])
        time.sleep(20)


def agentmonitorscheduler():
    try:
        print "agentmonitorscheduler is running.."
        agentmonitordaemon()

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
        my_logger.info("Calling itself.. agentmonitorscheduler")
        agentmonitorscheduler()