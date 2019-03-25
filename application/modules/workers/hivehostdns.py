import os

from application.common.loggerfile import my_logger
from bson.objectid import ObjectId
import pymongo
import sys
from application.configfile import mongo_conn_string
payloadid=sys.argv
object_id=payloadid[2]
def host_agent(objectid):
    try :
        myclient = pymongo.MongoClient(mongo_conn_string)
        mydb = myclient["haas"]
        querystatment=mydb.hiveconfig.find_one({"_id" : ObjectId(objectid)})
        statement='echo "'+querystatment["namenode_ip"]+'" >> /etc/hosts'
        execute=os.system(statement)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)


host_agent(objectid=object_id)


