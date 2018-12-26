import json
import requests
import logging
logging.basicConfig()
from application.configfile import server_url,agentinfo_path,agentregistration_connection

def agentregisterfunc():
	try:
		print 'in agent register daemon'
		agent_data=open(agentinfo_path,"r")
		content=agent_data.read()
		print content
		agent_data.close()
		data_req=json.loads(content,'utf-8')
		url = server_url+agentregistration_connection
		data = json.dumps(data_req)
		print 'data'
		headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
		requests.post(url,data=data,headers=headers)
		print 'done'


	except Exception as e:
		print e.message
