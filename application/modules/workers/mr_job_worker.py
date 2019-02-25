import requests
import json
from application.configfile import server_url
from application.common.job_management import MapRedResourceManager
import time
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
def mrjobworker(request_id):
	try:
		db_session = scoped_session(session_factory)
		maprjob_data = db_session.query(TblMrJobInfo.var_resourcemanager_ip,TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id,TblMrJobInfo.var_file_name,TblMrJobInfo.var_job_parameters).filter(TblMrJobInfo.uid_request_id == request_id).all()
		time_stamp=str(int(round(time.time() * 1000)))
		IPAddr = maprjob_data[0][0]
		customerid = maprjob_data[0][1]
		clusterid = maprjob_data[0][2]
		file_name = maprjob_data[0][3]
		job_parameters =maprjob_data[0][4]
		mr_parameters=job_parameters.replace(",","  ")
		mapred = MapRedResourceManager(address=IPAddr, port=8088)
		print IPAddr
		application_id=mapred.submitJob(jar_path="/opt/mnt/azurefileshare/sampledir/"+file_name,job_parameters=mr_parameters)
		print application_id
		object = db_session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id == request_id)
		object.update({"var_application_id": application_id})
		db_session.commit()
		db_session.close()
		mrjob_data={}
		mrjob_data["request_id"]=str(request_id)
		mrjob_data["customer_id"]=str(customerid)
		mrjob_data["cluster_id"]=str(clusterid)
		mrjob_data["application_id"]=str(application_id)
		mrjob_data["status"]='SUBMITTED'
		url=server_url+"api/jobupdation"
		headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
		r = requests.post(url, data=json.dumps(mrjob_data), headers=headers)
        url=server_url+"api/jobupdation"
        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
		r = requests.post(url, data=json.dumps(mrjob_data), headers=headers)
	except Exception as e:
             exc_type, exc_obj, exc_tb = sys.exc_info()
             fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
             my_logger.error(exc_type)
             my_logger.error(fname)
             my_logger.error(exc_tb.tb_lineno)
	finally:
		db_session.close()