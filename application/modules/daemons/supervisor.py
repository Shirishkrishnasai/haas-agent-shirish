import os
import json
import sqlite3
import datetime
import subprocess
from kafka import KafkaConsumer
from application import sqlite_string, db
from application.configfile import agentinfo_path, kafka_server_url
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus
import sys
import time
from sqlalchemy.orm import scoped_session
from application import session_factory

def supervisoragent():
    while True:
        try:
            my_logger.debug('in supervisor')
            info = open(agentinfo_path, "r")
            content = info.read()
            data_req = json.loads(content, 'utf-8')
            agent_id = str(data_req['agent_id'])
            consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
            consumer.subscribe(pattern='task*', )
            try:
                message = consumer.poll(timeout_ms=1000, max_records=1)
                if message != {}:
                    topicMesages = message.values()

                    for messageValues in topicMesages[0]:

                        session = scoped_session(session_factory)
                        consumer_data = messageValues.value
                        data = consumer_data.replace("'", '"')
                        tasks_data = json.loads(data)
                        if tasks_data['event_type'] == "tasks":
                            if agent_id == tasks_data['agent_id']:
                                my_logger.debug("agent_id verification done.....and this is true agent")
                                task_id_dict = tasks_data['task_id']
                                task_id = str(task_id_dict)
                                path = tasks_data['worker_path']
                                payloadid = tasks_data['payload_id']

                                starttime = datetime.datetime.now()
                                task_status_insert_data = TblAgentTaskStatus(var_task_status='running',
                                                                             ts_execution_start_datetime=starttime,
                                                                             uid_task_id=task_id,
                                                                             bool_flag=0
                                                                             )

                                session.add(task_status_insert_data)
                                session.commit()
                                my_logger.debug(
                                    "running status is updated in task status and bool flag is set to false as this is new entry")

                                if payloadid == None:
                                    my_logger.debug("there is no payload id that is y this block is being executed now")

                                    pathlist = path.split("/")
                                    pythonfile_name = pathlist[-1]
                                    extension_name = pythonfile_name.split(".")

                                    if extension_name[1] == 'py':
                                        py_path = "'" + "python" + " " + path + "'"
                                        my_logger.debug("this is python file")
                                        my_logger.debug(py_path)
                                        execute = os.system(py_path)
                                    else:
                                        sh_path = "sh" + " " + path
                                        my_logger.debug("this is shell script")
                                        my_logger.debug(sh_path)
                                        execute = os.system(sh_path)
                                        # Updates status if execution is completed
                                    if execute == 0:
                                        my_logger.debug("execution success")
                                        endtime = datetime.datetime.now()
                                        update_task_status_query = session.query(TblAgentTaskStatus).filter(
                                            TblAgentTaskStatus.uid_task_id == task_id)
                                        update_task_status_query.update({"var_task_status": "completed",
                                                                         "bool_flag": 0,
                                                                         "ts_finished_datetime": endtime})
                                        session.commit()
                                        my_logger.debug("completed status inserted into database")
                                # Assigning to worker if worker has arguments to be taken

                                else:

                                    my_logger.debug("there is payload id that is y this block is being executed")
                                    pathlist = path.split("/")
                                    pythonfile_name = pathlist[-1]
                                    name = pythonfile_name.split(".")
                                    py_path = []
                                    if name[1] == 'py':

                                        py_path.append("python")
                                        py_path.append(path)
                                        py_path.append("payload_id")
                                        py_path.append(payloadid)
                                        my_logger.debug("this is python file")
                                        my_logger.debug(py_path)
                                        execute = subprocess.call(py_path, shell=False)
                                        my_logger.debug(execute)
                                        my_logger.debug("that is execuute output for python file")
                                    else:
                                        sh_path = []
                                        sh_path.append('. ')
                                        sh_path.append(path)
                                        sh_path.append("payload_id")
                                        sh_path.append(payloadid)
                                        my_logger.debug(sh_path)
                                        execute = subprocess.call(sh_path, shell=True)
                                    if execute == 0:
                                        endtime = datetime.datetime.now()

                                        update_task_status_query = session.query(TblAgentTaskStatus).filter_by(
                                            uid_task_id=str(task_id))
                                        update_task_status_query.update({"var_task_status": "completed",
                                                                         "bool_flag": 0,
                                                                         "ts_finished_datetime": endtime})
                                        session.commit()
                                        my_logger.debug("last statement in supervisor...updation done")
                        session.close()
            except Exception as e:
                my_logger.debug("Erro Occured ad supervusir, at 125")
                my_logger.debug(sys.exc_info()[0])
                my_logger.error(e.message)
        except Exception as e:
            my_logger.debug(sys.exc_info()[0])
            my_logger.error(e.message)
        finally:
            consumer.close()
    time.sleep(10)
