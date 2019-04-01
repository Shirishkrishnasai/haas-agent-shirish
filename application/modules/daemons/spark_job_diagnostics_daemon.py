import requests, json, os, sys
from application.configfile import server_url,agentinfo_path
from sqlalchemy.orm import scoped_session
from application import session_factory
from sqlalchemy import and_
from application.models.models import TblSparkJobInfo
from apscheduler.schedulers.background import BackgroundScheduler
from application.common.loggerfile import my_logger
from application.configfile import server_url


def spark_diagnostics():
    try:
       session = scoped_session(session_factory)
       agent_data = open(agentinfo_path, 'r')
       content = agent_data.read()
       agent_data.close()
       req_data = json.loads(content, 'utf-8')
       role = req_data['role']
       if role != 'spark':
           pass
       else:
            diagnostics_data = session.query(TblSparkJobInfo.uid_request_id,TblSparkJobInfo.var_application_id,TblSparkJobInfo.uid_customer_id,TblSparkJobInfo.uid_cluster_id,TblSparkJobInfo.var_status,TblSparkJobInfo.var_resourcemanager_ip).filter(and_(TblSparkJobInfo.var_status !='success',TblSparkJobInfo.var_status !='dead')).all()
            if diagnostics_data != []:
                for spark_details in diagnostics_data:
                    app_id = str(spark_details[1])
                    if app_id == 'None':
                        print '\n'
                        print app_id,"NO Application-ID found"
                        pass
                    else:
                        data={}
                        resourcemanager_url = "http://"+diagnostics_data[0][5]+":8088/ws/v1/cluster/apps/"+app_id
                        job_data = requests.get(resourcemanager_url)
                        data1 =job_data.json()
                        data['cluster_id'] = str(diagnostics_data[0][3])
                        data['customer_id'] = str(diagnostics_data[0][2])
                        data['request_id'] = str(diagnostics_data[0][0])
                        data['diagnostics'] = str(data1)

                        url=server_url + "/sparkjobdiagnostics"
                        out_data = json.dumps(data)
                        header = {'content-type': 'application/json', 'Accept': 'text/plain'}
                        s = requests.post(url, data=out_data, headers=header)
                        response = s.json()
                        print response
            else:
                print "no dataaa found"

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        session.close()

def sparkjobdiagnosticscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(spark_diagnostics, 'cron', second='*/10')
    scheduler.start()
