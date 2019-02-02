from kafka import KafkaConsumer
from application.configfile import kafka_bootstrap_server
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
from application.modules.workers.mr_job_worker import mrjobworker
from application.configfile import server_url
import requests

import json

def insertjob():
    try:
        session = scoped_session(session_factory)
        url = server_url + 'api/mrjob'
        print url, " this agent daemon just called the hgmanager apiiiiiii"

        r = requests.get(url)
	print r
        req_data = r.json()
        print (req_data), type(req_data)
        for data in req_data['message']:
            print data

         #   if data['agent_id'] == agent_id:
            insert_mr_job_info=TblMrJobInfo(var_resourcemanager_ip=data['resourcemanager_ip'],
                                            uid_request_id=data['request_id'],
                                            uid_customer_id=data['customer_id'],
                                            uid_cluster_id=data['cluster_id'],
                                            uid_conf_upload_id=data['uid_conf_upload_id'],
                                            uid_jar_upload_id=data['uid_jar_upload_id'],
                                            var_job_status='CREATED',bool_job_status_produce=0,
                                            var_job_diagnostic_status='CREATED')
            session.add(insert_mr_job_info)
            print "add"
            session.commit()
            print "inserted"
            mrjobworker(data['request_id'])
            print "commited"
    except Exception as e:
      print e.message
