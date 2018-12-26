import os
from bson.objectid import ObjectId
import pymongo
import sys
from application.configfile import mongo_conn_string
payloadid=sys.argv
object_id=payloadid[2]
def worker_agent(objectid):
    myclient = pymongo.MongoClient(mongo_conn_string)
    mydb = myclient["haas"]
    print(objectid)
    querystatment=mydb.slaves.find_one({"_id" : ObjectId(objectid)})
    #slave=open("slaves","w")
    print(querystatment["content"])
    #slave.write("%s" % querystatment["content"])
    #slave.close()
    print(querystatment["content"],"123")
    data=querystatment["content"]
    statement = 'echo "' + data + '" > /opt/hadoop/etc/hadoop/slaves'
    execute=os.system(statement)
worker_agent(objectid=object_id)

