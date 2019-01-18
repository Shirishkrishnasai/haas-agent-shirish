import datetime
import json
import os
import subprocess
import sys
import time

from application import session_factory
from application.common.load_config import loadconfig
from application.common.loggerfile import my_logger
from application.configfile import kafka_server_url
from application.models.models import TblAgentTaskStatus
from kafka import KafkaConsumer
from sqlalchemy.orm import scoped_session


def _supervisoragent():
    my_logger.debug('in supervisor')
    #calling function for agent info details
    agent_id, customer_id, cluster_id = loadconfig()

    consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
    consumer.subscribe(pattern='task*')
    while True:
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
                            my_logger.info("agent_id verification done.....and this is true agent")
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
                            my_logger.info(
                                "running status is updated in task status and bool flag is set to false as this is new entry")

                            if payloadid == None:
                                my_logger.info("there is no payload id that is y this block is being executed now")

                                pathlist = path.split("/")
                                pythonfile_name = pathlist[-1]
                                extension_name = pythonfile_name.split(".")

                                if extension_name[1] == 'py':
                                    py_path = "'" + "python" + " " + path + "'"
                                    my_logger.info("this is python file")
                                    my_logger.info(py_path)
                                    execute = os.system(py_path)
                                else:
                                    sh_path = "sh" + " " + path
                                    my_logger.info("this is shell script")
                                    my_logger.info(sh_path)
                                    execute = os.system(sh_path)
                                    # Updates status if execution is completed
                                if execute == 0:
                                    my_logger.info("execution success")
                                    endtime = datetime.datetime.now()
                                    update_task_status_query = session.query(TblAgentTaskStatus).filter(
                                        TblAgentTaskStatus.uid_task_id == task_id)
                                    update_task_status_query.update({"var_task_status": "completed",
                                                                     "bool_flag": 0,
                                                                     "ts_finished_datetime": endtime})
                                    session.commit()
                                    my_logger.info("completed status inserted into database")
                            # Assigning to worker if worker has arguments to be taken

                            else:

                                my_logger.info("there is payload id that is y this block is being executed")
                                pathlist = path.split("/")
                                pythonfile_name = pathlist[-1]
                                name = pythonfile_name.split(".")
                                py_path = []
                                if name[1] == 'py':

                                    py_path.append("python")
                                    py_path.append(path)
                                    py_path.append("payload_id")
                                    py_path.append(payloadid)
                                    my_logger.info("this is python file")
                                    my_logger.info(py_path)
                                    execute = subprocess.call(py_path, shell=False)
                                    my_logger.info(execute)
                                    my_logger.info("that is execuute output for python file")
                                else:
                                    sh_path = []
                                    sh_path.append('. ')
                                    sh_path.append(path)
                                    sh_path.append("payload_id")
                                    sh_path.append(payloadid)
                                    my_logger.info(sh_path)
                                    execute = subprocess.call(sh_path, shell=True)
                                if execute == 0:
                                    endtime = datetime.datetime.now()

                                    update_task_status_query = session.query(TblAgentTaskStatus).filter_by(
                                        uid_task_id=str(task_id))
                                    update_task_status_query.update({"var_task_status": "completed",
                                                                     "bool_flag": 0,
                                                                     "ts_finished_datetime": endtime})
                                    session.commit()
                                    my_logger.info("last statement in supervisor...updation done")
                    session.close()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
        finally:
            consumer.close()
        time.sleep(10)


def supervisoragent():
    try:
        _supervisoragent()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
        my_logger.info("Calling itself.. supervisorAgent")
    # supervisoragent()
