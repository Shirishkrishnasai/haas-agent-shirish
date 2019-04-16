import json, os, sys, requests, multiprocessing
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.configfile import server_url,agentinfo_path
from application.common.loggerfile import my_logger
from apscheduler.schedulers.background import BackgroundScheduler
from application.models.models import TblSparkJobInfo
from application.modules.workers.spark_job_worker import spark_submit

def insertjob():
    try:
        session = scoped_session(session_factory)
        agent_data = open(agentinfo_path,'r')
        content = agent_data.read()
        agent_data.close()
        req_data = json.loads(content, 'utf-8')
        agent_id = req_data['agent_id']
        role = req_data['role']
        if role != "spark":
            pass
        else:
            url = server_url + 'sparkjob/'+agent_id
            r = requests.get(url)
            req_data=r.json()
            if req_data != 'null':
                for data in req_data['message']:
                    insert_spark_job_info = TblSparkJobInfo(uid_request_id=str(data['request_id']),
                                                            uid_customer_id=data['customer_id'],
                                                            uid_cluster_id=data['cluster_id'],
                                                            var_file_name=data['filename'],
                                                            var_spark_job_description=data['job_description'],
                                                            var_spark_job_parameters=data['job_parameters'],
                                                            var_spark_job_status='CREATED',
                                                            uid_jar_upload_id=data['jar_id'],
                                                            bool_job_status_produce=0,
                                                            var_spark_job_diagnostic_status='CREATED',
                                                            var_status='initialized',
                                                            var_resourcemanager_ip=str(data['resourcemanager_ip']))

                    session.add(insert_spark_job_info)
                    session.commit()
                    submit_process = multiprocessing.Process(target=spark_submit,
                                                             args=([data['request_id']]))
                    submit_process.start()
                    submit_process.join()

            else:
                pass
            print "doneeeeeeeeeeeeee"

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        session.close()




def sparkjobinsertionscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(insertjob, 'cron', second='*/20')
    scheduler.start()
