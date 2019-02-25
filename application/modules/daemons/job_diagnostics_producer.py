from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
from apscheduler.schedulers.background import BackgroundScheduler
from application.configfile import  server_url
import json, sys, os
import requests
from application.common.loggerfile import my_logger


def jobdiagnostics():
    try:
        session = scoped_session(session_factory)

        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,TblMrJobInfo.var_job_status,
                                     TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).\
                                     filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED').all()
        for job_details in job_info_query:
            appnumber=job_details[1].split("_")
            appincrement=int(appnumber[-1])+1
            applicationid=str(appincrement).zfill(4)
            replaceappid=job_details[1].replace(str(appnumber[-1]),str(applicationid))
            resource_manager_url = "http://"+str(job_details[5])+":8088/ws/v1/cluster/apps/"+replaceappid
            token = requests.get(resource_manager_url)
            data = token.json()

            data['customer_id'] = str(job_details[3])
            data['cluster_id']  = str(job_details[4])
            data['request_id']  = str(job_details[0])

            url = server_url + 'api/jobdiagnostics'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            requests.post(url, data=json.dumps(data), headers=headers)
            update_job_info_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.var_application_id==job_details[1])
            update_job_info_query.update({"bool_job_status_produce":1})
            session.commit()
    except Exception as e:
         exc_type, exc_obj, exc_tb = sys.exc_info()
         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

         my_logger.error(exc_type)
         my_logger.error(fname)
         my_logger.error(exc_tb.tb_lineno)
    finally:
        session.close()
def jobdiagnosticsscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(jobdiagnostics, 'cron', second='*/20')
    scheduler.start()


