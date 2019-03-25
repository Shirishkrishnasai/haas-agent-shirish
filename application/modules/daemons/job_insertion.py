import os,sys
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
from application.modules.workers.mr_job_worker import mrjobworker
from apscheduler.schedulers.background import BackgroundScheduler
from application.configfile import server_url,agentinfo_path
import requests
from application.common.loggerfile import my_logger
import json
def insertjob():
   try:
        session = scoped_session(session_factory)
        agent_data = open(agentinfo_path, "r")
        content = agent_data.read()
        agent_data.close()
        data_req = json.loads(content, 'utf-8')
        data_req["role"]
        if data_req["role"] != "namenode":
                pass
        else:
                url = server_url + 'api/mrjob'
                r = requests.get(url)
                req_data = r.json()
                for data in req_data['message']:
                        insert_mr_job_info=TblMrJobInfo(var_resourcemanager_ip=data['resourcemanager_ip'],
                                            uid_request_id=data['request_id'],
                                            uid_customer_id=data['customer_id'],
                                            uid_cluster_id=data['cluster_id'],
                                                                var_file_name=data['filename'],
                                                                var_job_description=data['job_description'],
                                            var_job_parameters=data['job_parameters'],
                                            var_job_status='CREATED',bool_job_status_produce=0,
                                            var_job_diagnostic_status='CREATED')
                        session.add(insert_mr_job_info)
                        session.commit()
                        mrjobworker(data['request_id'])
   except Exception as e:
           exc_type, exc_obj, exc_tb = sys.exc_info()
           fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
           my_logger.error(exc_type)
           my_logger.error(fname)
           my_logger.error(exc_tb.tb_lineno)
   finally:
        session.close()


def jobinsertionscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(insertjob, 'cron', second='*/20')
    scheduler.start()

