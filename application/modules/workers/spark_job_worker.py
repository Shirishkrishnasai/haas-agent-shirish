import json, requests, os, sys
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblSparkJobInfo
from application.common.loggerfile import my_logger
from subprocess import call

command= 'sudo /opt/hadoop/bin/hdfs dfs -moveFromLocal /opt/mnt/azurefileshare/spark/* /spark/'
call(command, shell=True)

def spark_submit(request_id):
        try:
                db_session = scoped_session(session_factory)
                sparkjob_data = db_session.query(TblSparkJobInfo.uid_cluster_id,TblSparkJobInfo.uid_customer_id,TblSparkJobInfo.var_file_name,TblSparkJobInfo.var_spark_job_parameters).filter(TblSparkJobInfo.uid_request_id == request_id).all()
                file_name = sparkjob_data[0][2]
                job_parameter = sparkjob_data[0][3]
                spark_data = {}
                spark_data["file"] = "/spark/"+str(file_name)
                spark_data["className"] = str(job_parameter)
                print spark_data
                host = "http://localhost:8998"
                headers = {'Content-Type': 'application/json'}
                r = requests.post(host + '/batches', data=json.dumps(spark_data), headers=headers)
                var=r.json()
                status = var['state']
                id = var['id']
                print "id =",var['id']
                print "state =",var['state']
                spark_update = db_session.query(TblSparkJobInfo).filter(TblSparkJobInfo.uid_request_id==request_id)
                spark_update.update({"var_status": status,"var_id":id})
                db_session.commit()

        except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                my_logger.error(exc_type)
                my_logger.error(fname)
                my_logger.error(exc_tb.tb_lineno)

        finally:
                db_session.close()








