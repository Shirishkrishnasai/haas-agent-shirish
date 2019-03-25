from application.common.loggerfile import my_logger
from bson.objectid import ObjectId
import pymongo
import sys,os
import subprocess
from application.configfile import mongo_conn_string

payloadid = sys.argv
object_id = payloadid[2]


def worker_agent(objectid):
    try :
        myclient = pymongo.MongoClient(mongo_conn_string)
        mydb = myclient["haas"]
        querystatment = mydb.configurenamenode.find_one({"_id": ObjectId(objectid)})
        ip = querystatment["content"]
        path = "bash /opt/scripts/configuration.sh" + " " + str(ip)
        sh_path = []
        sh_path.append(path)
        execute = subprocess.call(sh_path, shell=True)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
    finally :
        myclient.close()
worker_agent(objectid=object_id)
