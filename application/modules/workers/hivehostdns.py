import os
from bson.objectid import ObjectId
import pymongo
import sys
from application.configfile import mongo_conn_string
payloadid=sys.argv
object_id=payloadid[2]
def host_agent(objectid):
    myclient = pymongo.MongoClient(mongo_conn_string)
    mydb = myclient["haas"]
    print(objectid)
    querystatment=mydb.hiveconfig.find_one({"_id" : ObjectId(objectid)})
    #host=open("hosts","w")
    print(querystatment["namenode_ip"])
    #host.write("%s" % querystatment["content"])
    #host.close()
    print(querystatment["namenode_ip"],"123")
    statement='echo "'+querystatment["namenode_ip"]+'" >> /etc/hosts'
    print statement
    execute=os.system(statement)
    print execute
host_agent(objectid=object_id)


