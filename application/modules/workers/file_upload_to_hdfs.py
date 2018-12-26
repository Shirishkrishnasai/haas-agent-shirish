from bson.objectid import ObjectId
import pymongo
import sys
import subprocess
from application.configfile import mongo_conn_string,agentinfo_path
import urllib2
import requests
import os



#payloadid = sys.argv
#upload_id = payloadid[2]

def fileuploadhdfs(uploadid):
 #   file=open(agentinfo_path,"r")
 #   contents=file.read()
 #   u=contents.decode('utf-8-sig')
 #   contents=u.encode('utf-8')
  #  file.encoding
  #  file.close()
   # data_req=json.loads(contents,'utf-8')
    #print data_req
    #agentid=data_req['agent_id']
   agentid='2e4d1c54-ea26-11e8-ada4-3ca9f49ab2cf'
   file_url="http://192.168.100.164:5000/filedownload/"+str(uploadid)+"?agent_id="+agentid
    #response=urllib2.urlopen(process)
    #file_url=response.read()
   # print file_url,'file'
   r = requests.get(file_url)
   read_file=requests.get(r.text)
   data=str(read_file.text)
   msg=len(data.encode('utf-8'))
   print msg
   size=0
   size1=0
   lines = "sadfgasdhfrafhfwarfasfgsafgsafkskvnmbmhesrfsmfvskdfhsdfhjfhsx jhnsadyferjweureeueryfchjududususududududuvjchxtvsh7fdiguvkjdc8ffihsjdccurcfwn"


   os.popen("echo -e " + str(lines) + " | hadoop dfs -put - /readfile_sample")

#while size <= len(data.encode('utf-8')):

      #size=100+size
#      datatrimmed=data[size1:size]
 #     print "from",datatrimmed,"yes"
      #for lines in data.split('\n'):
       #  print lines
   #openfile=open("file3.txt","wr")
   #openfile.write(msg)
   #openfile.close()
   #file=open("file3.txt")
   #print len(file.read())
   #size=0
   #if filesize <= len(file.read()):
   #   size=size+filesize

 #print msg.read(300)

 #   print r.iter_content(chunk_size=1024)
