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
    querystatment=mydb.hostdns.find_one({"_id" : ObjectId(objectid)})
    #host=open("hosts","w")
    print(querystatment["content"])
    #host.write("%s" % querystatment["content"])
    #host.close()
    print(querystatment["content"],"123")
    statement='echo "'+querystatment["content"]+'" >> /etc/hosts'
    print statement
    execute=os.system(statement)
    print execute
host_agent(objectid=object_id)


