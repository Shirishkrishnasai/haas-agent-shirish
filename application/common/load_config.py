import json,os,sys
from application.common.loggerfile import my_logger
from application.configfile import agentinfo_path


def loadconfig():
    try:
        info = open(agentinfo_path, "r")
        content = info.read()
        data_req = json.loads(content, 'utf-8')
        agent_id = str(data_req['agent_id'])
        customer_id = str(data_req['customer_id'])
        cluster_id = str(data_req['cluster_id'])
        return agent_id,customer_id,cluster_id
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(str(e))
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
        my_logger.error("Sometehing went worng while parsing agentinfo file...")
        return None,None,None
