from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
import subprocess
from application.config.configfile import kafka_bootstrap_server, kafka_api_version
from kafka import KafkaProducer


def jobdiagnostics():
    try:
        session = scoped_session(session_factory)
        job_info_query=session.query(TblMrJobInfo.uid_request_id,TblMrJobInfo.var_application_id,TblMrJobInfo.var_job_status,TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_resourcemanager_ip).filter(TblMrJobInfo.var_job_status !='FAILED',TblMrJobInfo.var_job_status != 'FINISHED')

        for job_details in job_info_query:
            topic_string="job_diagnostics"+"_"+job_details[3]+"_"+job_details[4]

            diagnostic_process=subprocess.Popen("curl http://"+job_details[6]+":8088/proxy/"+job_details[1]+"/ws/v1/mapreduce/jobs",shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            token,err=diagnostic_process.communicate()
            token['customer_id']=job_details[3]
            token['cluster_id']=job_details[4]
            token['request_id']=job_details[0]
            producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
            kafkatopic = topic_string.decode('utf-8')
            producer.send(kafkatopic,str(token))
            producer.flush()
            print "flusheddddddddddddddssssssssssssssssss"
            update_job_info_query = session.query(TblMrJobInfo).filter(TblMrJobInfo.var_application_id==job_details[1])
            update_job_info_query.update({"bool_job_status_produce":"t"})
            session.commit()
    except Exception as e:
		return e.message
