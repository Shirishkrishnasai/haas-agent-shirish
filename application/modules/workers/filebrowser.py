import requests
import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from application.configfile import agentinfo_path,kafka_bootstrap_server,kafka_api_version
from application.common.loggerfile import my_logger

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
            my_logger.info('hiiiiiiiiiiiiiiiiiiiiiiii')
            hdfspath = message.value
            my_logger.info(type(hdfspath))
            data = hdfspath.replace("'", '"')
            #my_logger.info(data)
            message = json.loads(data)
            #my_logger.info(message ,type(message) ,'message',message.keys()
            filename=message["filename"]
            clusterid=message['cluster_id']
            customerid=message['customer_id']
            agentid=message['agent_id']
            timestamp=message['timestamp']
            namenodeip=message['namenode_ip']
            my_logger.info(agentid)
            if agentid==agent_id:
                my_logger.info(agentid)
                response = requests.get(url="http://"+namenodeip+":50070/webhdfs/v1/"+filename+"?op=LISTSTATUS")
                result= response.text
                result=json.loads(result)
                #my_logger.info(result
                file_list=result["FileStatuses"]["FileStatus"]
                data=[]
                for file in file_list:
                    files_data={}
                    files_data["size"]=str(file["length"])
                    files_data["filepath"]=str(file["pathSuffix"])
                    files_data["type"]=str(file["type"])
                    data.append(files_data)
                #my_logger.info(data,'daaaaaaaaaaaataaaaaaaaaaaaaaaaa'

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
                my_logger.info('flush')
    except Exception as e:
        my_logger.info(e.message)