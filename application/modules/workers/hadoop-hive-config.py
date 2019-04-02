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

    querystatment = mydb.hiveconfig.find_one({"_id": ObjectId(objectid)})
    print(querystatment["namenode_ip"])
    ip = querystatment["namenode_ip"]

    path = "bash /opt/scripts/hive-hadoop-config.sh" + " " + str(ip)

    sh_path = []
    print(path)
    sh_path.append(path)
    print(sh_path)
    execute = subprocess.call(sh_path, shell=True)

    print(execute)


worker_agent(objectid=object_id)

