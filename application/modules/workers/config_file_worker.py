import requests
import json
import shutil
import os
import time
from sqlalchemy.orm import scoped_session
from application import session_factory
from application.models.models import TblMrJobInfo
from application.configfile import agentinfo_path
""""
Need to look
"""
def conf_file_download(request_id):
    info = open(agentinfo_path, "r")
    content = info.read()
    data_req = json.loads(content, 'utf-8')
    agent_id = str(data_req['agent_id'])
    db_session = scoped_session(session_factory)
    uid = db_session.query(TblMrJobInfo.uid_conf_upload_id).filter(TblMrJobInfo.uid_request_id == request_id).all()
    uid=uid[0][0]
    time_stamp=str(int(round(time.time() * 1000)))
    folder_name="job_"+time_stamp
    URL="http://192.168.100.175:5000/filedownload/"+uid
    #agentid='c28bef0e-de9a-11e8-917e-3ca9f49ab2cc'
    PARAMS= {'agent_id':agent_id}
    response=requests.get(url=URL, params=PARAMS)
    file_download_url= response.text
    file = requests.get(url=file_download_url,params=None)
    with open("mapred-site.xml", 'wb') as f:
        f.write(file.content)
    os.mkdir("/tmp/"+folder_name)
    shutil.move("mapred-site.xml","/tmp/"+folder_name+"/")
    object = db_session.query(TblMrJobInfo).filter(TblMrJobInfo.uid_request_id == request_id)
    object.update({"str_conf_folder_name":folder_name})
    db_session.commit()
    db_session.close()
#conf_file_download("2b56ccf4-e63c-11e8-91d3-3ca9f49ab2cc")


