import os
from bson.objectid import ObjectId
import pymongo
import sys
from application.configfile import mongo_conn_string
from application.common.loggerfile import my_logger

payloadid=sys.argv
object_id=payloadid[2]

def worker_agent(objectid):
    myclient = pymongo.MongoClient(mongo_conn_string)
    mydb = myclient["haas"]
    my_logger.info(objectid)
    querystatment=mydb.slaves.find_one({"_id" : ObjectId(objectid)})
    #slave=open("slaves","w")
    my_logger.info(querystatment["content"])
    #slave.write("%s" % querystatment["content"])
    #slave.close()
    my_logger.info(querystatment["content"],"123")
    data=querystatment["content"]
    statement = 'echo "' + data + '" > /opt/hadoop/etc/hadoop/slaves'
    execute=os.system(statement)
worker_agent(objectid=object_id)

