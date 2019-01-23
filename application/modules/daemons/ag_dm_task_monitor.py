import datetime
import json
import logging
import sys
import time

from kafka import KafkaProducer

logging.basicConfig()
import sqlite3
from application import sqlite_string, db
from application.configfile import kafka_server_url, agentinfo_path
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus
import os, sys

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
            my_logger.info(tasks)
            my_logger.info("agent task status daemon fetched tasks")
            for each_task in tasks:
                my_logger.info("in for loop")
                my_logger.info(each_task)
                task_data = {}
                if each_task[2] == 0:
                    my_logger.info("checked for bool_flag=0 in db")
                    task_data['task_id'] = each_task[0]
                    task_data['status'] = each_task[1]
                    kafka_topic = "taskstatus_" + customer_id + "_" + cluster_id
                    my_logger.info(kafka_topic)
                    kafkatopic = kafka_topic.decode('utf-8')
                    my_logger.info("this is kafkaaaaaaaaaaaa topic for status")
                    my_logger.info(kafkatopic)
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
                    my_logger.info("print all the data that has to be given from kafka status producer")
                    my_logger.info(task_status_data)


                    producer.send(kafkatopic, str(task_status_data))
                    producer.flush()
                    my_logger.info("doneeeeeeee-doneeeeeeeeeeee-londonnnnnnnnnnnnn")
                    my_logger.info("done for agent task monitor daemon to send task status data through kafka pipeline")
                    # url=server_url+hgmonitor_connection
                    # headers={'content-type':'application/json','Accept':'text/plain'}
                    # requests.post(url,data=json.dumps(task_data),headers=headers)
                    update_task_flag_query = db.session.query(TblAgentTaskStatus).filter(
                        TblAgentTaskStatus.uid_task_id == each_task[0])
                    update_task_flag_query.update({"bool_flag": 1})
                    db.session.commit()

                my_logger.info("everything doneeeeeeeeeeeeeeeeeeeeeeeeeee")
            print 'task update to hgmonitor successfull'
            producer.close()
        except sqlite3.Error as er:
            my_logger.info(er)
            my_logger.info(sys.exc_info()[0])
            # return 'sqlite error'
        except Exception as e:
            print  "Some this went wrong", sys.exc_info()[0]
            my_logger.info(e)
            my_logger.info(sys.exc_info()[0])
            # return e.message
        # producer.close()
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
