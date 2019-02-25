import os
from bson.objectid import ObjectId
import pymongo
import sys
from application.configfile import mongo_conn_string
from application.common.loggerfile import my_logger

payloadid=sys.argv
object_id=payloadid[2]
def host_agent(objectid):
    myclient = pymongo.MongoClient(mongo_conn_string)
    mydb = myclient["haas"]
    my_logger.info(objectid)
    querystatment=mydb.hostdns.find_one({"_id" : ObjectId(objectid)})
    #host=open("hosts","w")
    my_logger.info(querystatment["content"])
    #host.write("%s" % querystatment["content"])
    #host.close()
    my_logger.info(querystatment["content"],"123")
    statement='echo "'+querystatment["content"]+'" >> /etc/hosts'
    my_logger.info(statement)
    execute=os.system(statement)
    my_logger.info(execute)
host_agent(objectid=object_id)


