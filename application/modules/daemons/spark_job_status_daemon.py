import json, requests, os, sys
from application.configfile import server_url,agentinfo_path
from sqlalchemy import and_
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblSparkJobInfo
from apscheduler.schedulers.background import BackgroundScheduler
from application.common.loggerfile import my_logger


def spark_status_worker():
    try:
        db_session = scoped_session(session_factory)
        agent_data = open(agentinfo_path,'r')
        content = agent_data.read()
        agent_data.close()
        req_data = json.loads(content,'utf-8')
        role = req_data['role']
        if role != 'spark':
            pass
        else:
            sparkjob_data = db_session.query(TblSparkJobInfo.var_id,TblSparkJobInfo.uid_request_id).filter(and_(TblSparkJobInfo.var_status !='success',TblSparkJobInfo.var_status !='dead')).all()
            if sparkjob_data != None:
                for status in sparkjob_data:
                    var_id = status[0]
                    req_id = status[1]
                    host = "http://localhost:8998"
                    headers = {'Content-Type': 'application/json'}
                    r = requests.get(host + '/batches/'+ var_id, headers=headers)
                    data = r.json()
                    print "Application_ID:",data['appId']
                    spark_status_update = db_session.query(TblSparkJobInfo).filter(TblSparkJobInfo.uid_request_id == req_id)
                    spark_status_update.update({"var_status": data['state'],"var_application_id": data['appId']})
                    db_session.commit()
                    out_dict = {}
                    out_dict['request_id'] = req_id
                    out_dict['status'] = data['state']
                    out_dict['application_id'] = data['appId']
                    url = server_url + "sparkjobstatus"
                    data = json.dumps(out_dict)
                    header = {'content-type': 'application/json', 'Accept': 'text/plain'}
                    s = requests.post(url, data=data, headers=header)
                    response = s.json()
                    print response
            else:
                pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)

    finally:
        db_session.close()

def sparkjobstatusscheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(spark_status_worker, 'cron', second='*/10')
    scheduler.start()


