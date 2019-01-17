import json

from application.common.loggerfile import my_logger
from application.configfile import agentinfo_path


def loadconfig():
    try:
        info = open(agentinfo_path, "r")
        content = info.read()
        data_req = json.loads(content, 'utf-8')
        agent_id = str(data_req['agent_id'])
        return agent_id
    except Exception as e:
        my_logger.erro("Sometehing went worng while parsing agentinfo file...")
        return None
