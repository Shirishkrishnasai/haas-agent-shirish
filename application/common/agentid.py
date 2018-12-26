import json
from application.configfile import agentinfo_path


info = open(agentinfo_path, "r")
content = info.read()
data_req = json.loads(content, 'utf-8')
agent_id = str(data_req['agent_id'])
