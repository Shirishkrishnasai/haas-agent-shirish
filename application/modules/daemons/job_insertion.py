from kafka import KafkaConsumer
from application.configfile import kafka_bootstrap_server
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
from application.modules.workers.mr_job_worker import mrjobworker
import json
""""
Need to Look
"""
def insertjob():
    try:
        session = scoped_session(session_factory)
        consumer=KafkaConsumer(bootstrap_servers=kafka_bootstrap_server)
        consumer.subscribe(pattern='mrjob*')
        print consumer
#        info = open(agentinfo_path, "r")
 #       content = info.read()
  #      data_req = json.loads(content, 'utf-8')
  #      agent_id = str(data_req['agent_id'])
        for kafka_message in consumer:
            job_details=kafka_message.value
            data = job_details.replace("'", '"')
            message = json.loads(data)
         #   if message['agent_id'] == agent_id:
            insert_mr_job_info=TblMrJobInfo(var_resourcemanager_ip=message['resourcemanager_ip'],uid_request_id=message['request_id'],uid_customer_id=message['customer_id'],uid_cluster_id=message['cluster_id'],uid_conf_upload_id=message['uid_conf_upload_id'],uid_jar_upload_id=message['uid_jar_upload_id'],var_job_status='CREATED',bool_job_status_produce=0,var_job_diagnostic_status='CREATED')
            session.add(insert_mr_job_info)
            print "add"
            session.commit()
            print "inserted"
            mrjobworker(message['request_id'])
            print "commited"
    except Exception as e:
		return e.message
