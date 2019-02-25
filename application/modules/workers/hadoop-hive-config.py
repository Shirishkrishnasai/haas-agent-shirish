from bson.objectid import ObjectId
import pymongo
import sys
import subprocess
from application.configfile import mongo_conn_string
from application.common.loggerfile import my_logger

payloadid = sys.argv
object_id = payloadid[2]


def worker_agent(objectid):
    myclient = pymongo.MongoClient(mongo_conn_string)
    mydb = myclient["haas"]
    my_logger.info(objectid)

    querystatment = mydb.hiveconfig.find_one({"_id": ObjectId(objectid)})
    my_logger.info(querystatment["namenode_ip"])
    ip = querystatment["namenode_ip"]

    path = "bash /opt/scripts/hive-hadoop-config.sh" + " " + str(ip)

    sh_path = []
    my_logger.info(path)
    sh_path.append(path)
    my_logger.info(sh_path)
    execute = subprocess.call(sh_path, shell=True)

    my_logger.info(execute)


worker_agent(objectid=object_id)

