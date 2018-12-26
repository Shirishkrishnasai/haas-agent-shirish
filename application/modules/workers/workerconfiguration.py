from bson.objectid import ObjectId
import pymongo
import sys
import subprocess
from application.configfile import mongo_conn_string
payloadid = sys.argv
object_id = payloadid[2]

def worker_agent(objectid):

        
        myclient = pymongo.MongoClient(mongo_conn_string)
        mydb = myclient["haas"]
        print(objectid)

        querystatment = mydb.configurenamenode.find_one({"_id": ObjectId(objectid)})
        print(querystatment["content"])
        ip=querystatment["content"]
        
        path ="bash /opt/scripts/configuration.sh"+" "+str(ip)

        sh_path = []
        print(path)
        sh_path.append(path)
        print(sh_path)
        execute = subprocess.call(sh_path, shell=True)

        print(execute)
worker_agent(objectid=object_id)

