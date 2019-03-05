import requests
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from application.configfile import agentinfo_path,kafka_bootstrap_server,kafka_api_version
def webhdfs():
    try:
        #info = open(agentinfo_path, "r")
        #content = info.read()
        #data_req = json.loads(content, 'utf-8')
        #agent_id = str(data_req['agent_id'])
        agent_id='2e4d1c54-ea26-11e8-ada4-3ca9f49ab2cf'
        consumer = KafkaConsumer(bootstrap_servers=kafka_bootstrap_server)
        consumer.subscribe(pattern='filebrowsing*')
        for message in consumer:
            print 'hiiiiiiiiiiiiiiiiiiiiiiii'
            hdfspath = message.value
            print type(hdfspath)
            data = hdfspath.replace("'", '"')
            #print data,'dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
            message = json.loads(data)
            #print message ,type(message) ,'message',message.keys()
            filename=message["filename"]
            clusterid=message['cluster_id']
            customerid=message['customer_id']
            agentid=message['agent_id']
            timestamp=message['timestamp']
            namenodeip=message['namenode_ip']
            print agentid
            if agentid==agent_id:
                print agentid
                response = requests.get(url="http://"+namenodeip+":50070/webhdfs/v1/"+filename+"?op=LISTSTATUS")
                result= response.text
                result=json.loads(result)
                #print result
                file_list=result["FileStatuses"]["FileStatus"]
                data=[]
                for file in file_list:
                    files_data={}
                    files_data["size"]=str(file["length"])
                    files_data["filepath"]=str(file["pathSuffix"])
                    files_data["type"]=str(file["type"])
                    data.append(files_data)
                #print data,'daaaaaaaaaaaataaaaaaaaaaaaaaaaa'

                # producer part
                producer=KafkaProducer(bootstrap_servers=kafka_bootstrap_server)
                kafkatopic = "filestatus_" + customerid + "_" + clusterid
                encoded_data = str(data).encode('base64','strict')
                status_dict={}
                status_dict['result']=encoded_data
                status_dict['customerid']=str(customerid)
                status_dict['clusterid']=str(clusterid)
                status_dict['filename']=str(filename)
                status_dict['timestamp']=str(timestamp)

                kafkatopic = kafkatopic.decode('utf-8')
                producer.send(kafkatopic, str(status_dict))
                producer.flush()
                print 'flush'
    except Exception as e:
        print e.message