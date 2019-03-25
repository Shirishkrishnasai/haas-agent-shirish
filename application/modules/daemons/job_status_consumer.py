from kafka import KafkaConsumer
from application.config.config_file import kafka_bootstrap_server, kafka_api_version
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblCustomerJobRequest,TblMetaMrRequestStatus
import json
from datetime import datetime

def statusconsumer():
    try:
        session = scoped_session(session_factory)

        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap_server,group_id="job_Status"+str(datetime.now))
        consumer.subscribe(pattern='job_status*')
        for message in consumer:
            job_data = message.value
            data = job_data.replace("'", '"')
            json_loads_job_data = json.loads(data)
            meta_request_status_query = session.query(TblMetaMrRequestStatus.srl_id).filter(TblMetaMrRequestStatus.var_mr_request_status == json_loads_job_data['status'])

            update_customer_job_request=session.query(TblCustomerJobRequest).filter(TblCustomerJobRequest.uid_customer_id==json_loads_job_data['customer_id'],TblCustomerJobRequest.var_application_id==json_loads_job_data['application_id'])
           # print update_customer_job_request
            update_customer_job_request.update({"int_request_status":meta_request_status_query[0][0]})
            session.commit()
            print 'in'
    except Exception as e:
		return e.message
