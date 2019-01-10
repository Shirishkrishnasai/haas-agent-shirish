from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
import subprocess

from application.configfile import kafka_bootstrap_server, kafka_api_version
import re
import json
from kafka import KafkaProducer


def jobstatus():
    try:

        print "in job staus"
        session = scoped_session(session_factory)
        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,TblMrJobInfo.var_job_status,TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED')
        for job_details in job_info_query:
            print job_details
            topic_string="job_status"+"_"+job_details[3]+"_"+job_details[4]
            jobstatus=subprocess.Popen("curl http://"+job_details[5]+":8088/ws/v1/cluster/apps/" +job_details[1]+"/ state",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            token,err=jobstatus.communicate()
            print token
            jobstatus_dict=json.loads(token)
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
            kafkatopic = topic_string.decode('utf-8')
            string=json.dumps({"customer_id":job_details[3],"status": jobstatus_dict["app"]["state"], "request_id": job_details[0], "application_id": job_details[1]})
            print jobstatus_dict["app"]["state"]
            producer.send(kafkatopic,string)
            producer.flush()
            print "in"

            update_jobinfo_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id==job_details[0],TblMrJobInfo.var_application_id==job_details[1])
            update_jobinfo_query.update({"var_job_status":jobstatus_dict["app"]["state"]})
            session.commit()
    except Exception as e:
		return e.message
