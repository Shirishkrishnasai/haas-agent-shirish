from sqlalchemy.orm import scoped_session
from application import session_factory
from apscheduler.schedulers.background import BackgroundScheduler
from application.models.models import TblMrJobInfo
import subprocess
from application.configfile import server_url
import json, requests
import sys,os,json
from application.common.loggerfile import my_logger


def jobstatus():
    try:

        session = scoped_session(session_factory)
        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,
                                     TblMrJobInfo.var_job_status,TblMrJobInfo.uid_customer_id,
                                     TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).\
            filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED').all()
	job_list=[]
        for job_details in job_info_query:
            appnumber=job_details[1].split("_")
            appincrement=int(appnumber[-1])+1
            applicationid=str(appincrement).zfill(4)
            replaceappid=job_details[1].replace(str(appnumber[-1]),str(applicationid))
            jobstatus_statement="http://"+job_details[5]+":8088/ws/v1/cluster/apps/" +replaceappid+"/state"
            token=requests.get(jobstatus_statement)
            print token
            print type(token)
            jobstatus_dict=token.json()
            print jobstatus_dict
            application_data = {"customer_id":job_details[3],"status": jobstatus_dict["state"], "request_id": job_details[0], "application_id": job_details[1]}
            job_list.append(application_data)
            update_jobinfo_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id==job_details[0],TblMrJobInfo.var_application_id==job_details[1])
            update_jobinfo_query.update({"var_job_status":jobstatus_dict["state"]})
            session.commit()
        url = server_url + 'job_status'
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=json.dumps(job_list), headers=headers)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally:
         session.close()

def jobstatusscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(jobstatus, 'cron', second='*/20')
    scheduler.start()

