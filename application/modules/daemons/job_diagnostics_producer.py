from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
import subprocess
from application.configfile import kafka_bootstrap_server, kafka_api_version, server_url
from kafka import KafkaProducer
import json
import requests

def jobdiagnostics():
    #try:
        session = scoped_session(session_factory)

        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,TblMrJobInfo.var_job_status,
                                     TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).\
                                     filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED').all()
        print job_info_query,'job info query'
        for job_details in job_info_query:
            print job_details,'jobbbbbb'
            resource_manager_url = "http://"+str(job_details[5])+":8088/ws/v1/cluster/apps/"+str(job_details[1])
            print resource_manager_url
            token = requests.get(resource_manager_url)
            data = token.json()
            print data

            data['customer_id'] = str(job_details[3])
            data['cluster_id']  = str(job_details[4])
            data['request_id']  = str(job_details[0])

            url = server_url + 'api/jobdiagnostics'
            headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
            print url, json.dumps(data)
            requests.post(url, data=json.dumps(data), headers=headers)
            print 'postedddd'
            update_job_info_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.var_application_id==job_details[1])
            update_job_info_query.update({"bool_job_status_produce":1})
            session.commit()
    # except Exception as e:
		# return e.message


