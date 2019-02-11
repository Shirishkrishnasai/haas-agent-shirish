from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
import subprocess
from application.configfile import server_url
import json, requests


def jobstatus():
    try:

        print "in job status"
        session = scoped_session(session_factory)
        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,
                                     TblMrJobInfo.var_job_status,TblMrJobInfo.uid_customer_id,
                                     TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).\
            filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED').all()
        print job_info_query
        print "after query"
        for job_details in job_info_query:
            print job_details,'jobbbb'
            jobstatus=subprocess.Popen("curl http://"+job_details[5]+":8088/ws/v1/cluster/apps/" +job_details[1]+"/ state",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print "before comm"
            token,err=jobstatus.communicate()
            print token,err,"after comm"
            jobstatus_dict=json.loads(token)
            print jobstatus_dict,'job'
            data = {"customer_id":job_details[3],"status": jobstatus_dict["app"]["state"], "request_id": job_details[0], "application_id": job_details[1]}
            print 'after data'
            url = server_url + 'job_status'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            print url, json.dumps(data)
            requests.post(url, data=json.dumps(data), headers=headers)

            print "in"

            update_jobinfo_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id==job_details[0],TblMrJobInfo.var_application_id==job_details[1])
            update_jobinfo_query.update({"var_job_status":jobstatus_dict["app"]["state"]})
            session.commit()
    except Exception as e:
		return e.message

