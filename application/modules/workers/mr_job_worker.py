import requests
import json
import shutil
import os
from application.configfile import agentinfo_path,kafka_bootstrap_server, kafka_api_version,download_url,server_url

import conf
from application.common.job_management import MapRedResourceManager
import time
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
import  os,pwd

def mrjobworker(request_id):
    print request_id,'requestid'
    info = open('agent_info.txt', "r")
    print info,'info'
#    uid=os.system('whoami')
#    print uid
#    uid2 = pwd.getpwnam('hadoop')[2]
#    os.setuid(uid2)
    print 'os'
    content = info.read()
    data_req = json.loads(content, 'utf-8')
    agent_id = str(data_req['agent_id'])
    #agent_id='c28bef0e-de9a-11e8-917e-3ca9f49ab2cc'
    print agent_id,'agent'
    db_session = scoped_session(session_factory)
    uid = db_session.query(TblMrJobInfo.uid_conf_upload_id,TblMrJobInfo.uid_jar_upload_id).filter(TblMrJobInfo.uid_request_id == request_id).all()
    print uid,"uid query"
    conf_uid=uid[0][0]
    jar_uid=uid[0][1]
    print jar_uid,"jar uuid"
    time_stamp=str(int(round(time.time() * 1000)))
    folder_name="job_"+time_stamp
    print folder_name,"job folder name"
    URL=download_url+jar_uid
    #agentid='c28bef0e-de9a-11e8-917e-3ca9f49ab2cc'
    PARAMS= {'agent_id':agent_id}
    print URL,'URL'
    response=requests.get(url=URL, params=PARAMS)
    print response,"response"
    file_download_url= response.text
    file = requests.get(url=file_download_url,params=None)
    print file.text,"printed file"
    with open("mapred-site.xml", 'wb') as f:
        f.write(file.content)
    print 'map-red'
    os.mkdir("/tmp/"+folder_name)
    print 'himapred'
    shutil.move("mapred-site.xml","/tmp/"+folder_name+"/")
    print 'shuttle'
    URL = download_url+ jar_uid

    # agentid='c28bef0e-de9a-11e8-917e-3ca9f49ab2cc'
    PARAMS = {'agent_id': agent_id}
    response = requests.get(url=URL, params=PARAMS)
    file_download_url = response.text
    file = requests.get(url=file_download_url, params=None)
    print file,'sads'
    data = db_session.query(TblMrJobInfo.var_resourcemanager_ip,
                            TblMrJobInfo.uid_customer_id,TblMrJobInfo.uid_cluster_id).filter(TblMrJobInfo.uid_request_id == request_id).all()
    print data,"near data"

    IPAddr = "192.168.100.169"
    customerid= data[0][1]
    clusterid = data[0][2]
    print clusterid
    with open('mrjob.jar', 'wb') as f:
        f.write(file.content)
    print 'mr_job'
    print 'os'
    shutil.move("mrjob.jar", "/tmp/" + folder_name + "/")
    print 'hiii'
    print IPAddr
    mapred = MapRedResourceManager(address=IPAddr, port=8088)
    print mapred,'mapping'
#    folder_name='job_1549366992927'    
    application_id=mapred.submitJob(jar_path="/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar",filename="teragen",noofbytes=100000000,output="/test/job"+time_stamp)
    print application_id
    object = db_session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id == request_id)
    object.update({"var_application_id": application_id})
    db_session.commit()
    db_session.close()
    print 'home'


    mrjob_data={}
    mrjob_data["request_id"]=str(request_id)
    mrjob_data["customer_id"]=str(customerid)
    mrjob_data["cluster_id"]=str(clusterid)
    mrjob_data["agent_id"]=str(agent_id)
    mrjob_data["application_id"]=str(application_id)
    mrjob_data["status"]='SUBMITTED'
    url=server_url+"api/jobupdation"
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    print mrjob_data
    print conf.get('hadoop.http.staticuser.user')
    r = requests.post(url, data=json.dumps(mrjob_data), headers=headers)


    url=server_url+"/jobupdate"
    headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post(url, data=json.dumps(mrjob_data), headers=headers)


