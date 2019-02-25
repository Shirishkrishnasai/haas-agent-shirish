import datetime
import multiprocessing
import os
import subprocess
import sys

from application import session_factory
from application.common.load_config import loadconfig
from application.common.loggerfile import my_logger
from application.models.models import TblAgentTaskStatus,TblAgentWorkerTaskMapping
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import scoped_session


def _supervisoragent():
    try :
        db_session=scoped_session(session_factory)
        my_logger.info('in supervisor')
        my_logger.info("in gent supervisor")
        agent_id, customer_id, cluster_id = loadconfig()
        my_logger.info("agent info file information")
        my_logger.info(agent_id)
        my_logger.info(customer_id)
        my_logger.info(cluster_id)
        taskupdate= db_session.query(TblAgentWorkerTaskMapping.uid_task_id,TblAgentWorkerTaskMapping.txt_path,
                                     TblAgentWorkerTaskMapping.txt_payload_id).filter(TblAgentWorkerTaskMapping.var_task_status=="INITIALISED").all()
        my_logger.info(taskupdate)
        my_logger.info(type(taskupdate))
        tasks_data=[]
        for task in taskupdate:
            my_logger.info(task)
            tasks_dat={}
            tasks_dat['taskid']=str(task[0])
            tasks_dat['worker_path']=str(task[1])
            tasks_dat['payload_id']=str(task[2])
            tasks_data.append(tasks_dat)
        my_logger.info(tasks_data)
        if tasks_data!=[]:
            task_execution = multiprocessing.Process(target=runExecution,args=([tasks_data]))
            task_execution.start()
            task_execution.join()
        else :
            my_logger.info("no tasks to execute")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        db_session.close()

def supervisoragent():
    try:
        my_logger.info("supervisor executing its complicated and annoying method now")
        _supervisoragent()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
        my_logger.info("Calling itself.. supervisorAgent")



def runExecution(tasks_data):
    db_session = scoped_session(session_factory)
    try:
        my_logger.info("agent_id verification done.....and this is true agent")
        my_logger.info("supervisor------------agent_id verification done.....and this is true agent")
        for tasks in tasks_data :
            task_id = str(tasks['taskid'])
            my_logger.info(task_id)
            path = str(tasks['worker_path'])
            payloadid = str(tasks['payload_id'])
            starttime = datetime.datetime.now()
            task_status_insert_data = TblAgentTaskStatus(var_task_status='RUNNING',
                                                             ts_execution_start_datetime=starttime,
                                                             uid_task_id=task_id,
                                                             bool_flag=0
                                                             )

            db_session.add(task_status_insert_data)
            db_session.commit()
            task_update = db_session.query(TblAgentWorkerTaskMapping).filter(TblAgentWorkerTaskMapping.uid_task_id== task_id)
            task_update.update({"var_task_status": "RUNNING"})
            db_session.commit()
            my_logger.info("added")
            my_logger.info("running status is updated in task status and bool flag is set to false as this is new entry")
            my_logger.info("supervisor committed to database..........in supervisor.py")
            db_session = scoped_session(session_factory)
            if payloadid == str(None):

                my_logger.info("there is no payload id that is y this block is being executed now")
                my_logger.info("there is no payload id that is y this block is being executed now..........in supervisor.py")

                pathlist = path.split("/")
                pythonfile_name = pathlist[-1]
                extension_name = pythonfile_name.split(".")
                my_logger.info(pythonfile_name)
                if extension_name[1] == 'py':
                        py_path = "'" + "python" + " " + path + "'"
                        my_logger.info("this is python file")
                        my_logger.info(" its python file ---- chillax")
                        my_logger.info(py_path)
                        execute = os.system(py_path)
                else:
                        sh_path = "sh" + " " + path
                        my_logger.info("this is shell script")
                        my_logger.info("its shell script=================")
                        my_logger.info(sh_path)
                        execute = os.system(sh_path)
                        # Updates status if execution is completed
                if execute == 0:
                        my_logger.info("execution success")
                        my_logger.info("either file, execution is a success..........in supervisor.py")
                        endtime = datetime.datetime.now()
                        update_task_status_query = db_session.query(TblAgentTaskStatus).filter(
                            TblAgentTaskStatus.uid_task_id == task_id)
                        update_task_status_query.update({"var_task_status": "COMPLETED",
                                                         "bool_flag": 0,
                                                         "ts_finished_datetime": endtime})
                        db_session.commit()
                        my_logger.info("completed status inserted into database")
                        my_logger.info("database commit done by supervisor")
                # Assigning to worker if worker has arguments to be taken

            else:
                    my_logger.info("there is payload id that is y this block is being executed")

                    my_logger.info("there is payload id that is y this block is being executed..........in supervisor.py")
                    pathlist = path.split("/")
                    my_logger.info(path)
                    my_logger.info(pathlist)
                    pythonfile_name = pathlist[-1]
                    name = pythonfile_name.split(".")
                    py_path = []
                    if name[1] == 'py':
                        my_logger.info("hi im in if")
                        py_path.append("python")
                        py_path.append(path)
                        py_path.append("payload_id")
                        py_path.append(payloadid)
                        my_logger.info("this is python file")
                        my_logger.info(py_path)
                        execute = subprocess.call(py_path, shell=False)
                        my_logger.info(execute)
                        my_logger.info("that is execuute output for python file")
                        my_logger.info(execute)
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
                        taskstatusupdate = db_session.query(TblAgentTaskStatus).filter(TblAgentTaskStatus.uid_task_id == task_id)
                        taskstatusupdate.update({'var_task_status':'COMPLETED','bool_flag': 0,'ts_finished_datetime': endtime})
                        db_session.commit()
                        db_session.close()
                    my_logger.info("last statement in supervisor...updation done")
                    my_logger.info("last statement in supervisor...updation done.... supervisor.py completed")
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
        db_session.close()
def supervisorcheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(supervisoragent,'cron',second='*/7' )
    scheduler.start()
