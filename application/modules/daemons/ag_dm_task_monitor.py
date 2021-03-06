import datetime
import time
import requests
import json
from kafka import KafkaProducer
import logging
import sys
logging.basicConfig()
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from application import sqlite_string, db
from application.configfile import kafka_server_url, agentinfo_path, server_url, hgmonitor_connection
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus


def agentmonitordaemon():
    # getting status of worker and  uppdating hat information to hg monitor
    while True:
        try:

            producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
            info = open(agentinfo_path, "r")
            content = info.read()
            data_req = json.loads(content, 'utf-8')
            customer_id = str(data_req['customer_id'])
            cluster_id = str(data_req['cluster_id'])
            agent_id = str(data_req['agent_id'])

            conn = sqlite3.connect(sqlite_string)
            cur = conn.cursor()
            select_task_status = "select uid_task_id,var_task_status,bool_flag from tbl_agent_task_status"
            cur.execute(select_task_status)
            tasks = cur.fetchall()
            my_logger.debug(tasks)
            my_logger.debug("agent task status daemon fetched tasks")
            for each_task in tasks:
                my_logger.debug("in for loop")
                my_logger.debug(each_task)
                task_data = {}
                if each_task[2] == 0:
                    my_logger.debug("checked for bool_flag=0 in db")
                    task_data['task_id'] = each_task[0]
                    task_data['status'] = each_task[1]
                    kafka_topic = "taskstatus_" + customer_id + "_" + cluster_id
                    my_logger.debug(kafka_topic)
                    kafkatopic = kafka_topic.decode('utf-8')
                    my_logger.debug("this is kafkaaaaaaaaaaaa topic for status")
                    my_logger.debug(kafkatopic)
                    task_status_data = {}
                    task_status_data['event_type'] = "task_status"
                    date_time = datetime.datetime.now()
                    milliseconds_date = str(int(round(time.mktime(date_time.timetuple()))) * 1000)

                    task_status_data['time'] = milliseconds_date
                    task_status_data['customer_id'] = customer_id
                    task_status_data['cluster_id'] = cluster_id
                    task_status_data['agent_id'] = agent_id
                    payload_data = {}
                    payload_data['task_id'] = str(each_task[0])
                    payload_data['status'] = str(each_task[1])
                    payload_data['message'] = "incomplete for now"

                    task_status_data['payload'] = payload_data
                    my_logger.debug("print all the data that has to be given from kafka status producer")
                    my_logger.debug(task_status_data)

                    producer.send(kafkatopic, str(task_status_data))
                    producer.flush()
                    my_logger.debug("doneeeeeeee-doneeeeeeeeeeee-londonnnnnnnnnnnnn")
                    my_logger.debug("done for agent task monitor daemon to send task status data through kafka pipeline")
                    # url=server_url+hgmonitor_connection
                    # headers={'content-type':'application/json','Accept':'text/plain'}
                    # requests.post(url,data=json.dumps(task_data),headers=headers)
                    update_task_flag_query = db.session.query(TblAgentTaskStatus).filter(
                        TblAgentTaskStatus.uid_task_id == each_task[0])
                    update_task_flag_query.update({"bool_flag": 1})
                    db.session.commit()

                my_logger.debug("everything doneeeeeeeeeeeeeeeeeeeeeeeeeee")
            print 'task update to hgmonitor successfull'
            producer.close()
        except sqlite3.Error as er:
            my_logger.debug(er)
            my_logger.debug(sys.exc_info()[0])
            #return 'sqlite error'
        except Exception as e:
            print  "Some this went wrong", sys.exc_info()[0]
            my_logger.debug(e)
            my_logger.debug(sys.exc_info()[0])
            #return e.message
        #producer.close()
        time.sleep(20)

def agentmonitorscheduler():
    #scheduler = BackgroundScheduler()
    #scheduler.add_job(agentmonitordaemon, 'cron', minute='*/1')
    #scheduler.start()
    agentmonitordaemon()
