import json
import requests
import logging
logging.basicConfig()

#from apscheduler.schedulers.background import BackgroundScheduler
#from application import *
from application.configfile import server_url,agentinfo_path

def agentregisterfunc():
   try:
	info=open(agentinfo_path,"r")
	print info
	content=info.read()
	u=content.decode('utf-8-sig')
	content=u.encode('utf-8')
	info.encoding
	info.close()
	data_req=json.loads(content,'utf-8')

	#jsond_content=json.dumps(content)
	#jsonl_content=json.loads(jsond_content)
	#print jsonl_content
	#print type(jsonl_content)
	print data_req
	print type(data_req)


	url = server_url+'register'
	data = json.dumps(data_req)
	#d=json.loads(data)
	#print d
	#print json.dumps(data)
	print 'heyyyyyyyyyyyyyyyyy',data
	headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
	r = requests.post(url,data=data,headers=headers)
	print r
   except Exception as e:
	print e.message
