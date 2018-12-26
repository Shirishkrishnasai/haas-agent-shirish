import json
from kafka import KafkaConsumer
from kafka import KafkaProducer
from application.common.loggerfile import my_logger
from application.configfile import agentinfo_path, kafka_server_url,hive_connection
from application.common.hive import HiveQuery

def hiveDatabaseQueryConsumer():

    while True:
        try:

            print "haiiii in hive database query"
            my_logger.debug('in hive query consumer')
            info = open(agentinfo_path, "r")
            content = info.read()
            data_req = json.loads(content, 'utf-8')
            agent_id = str(data_req['agent_id'])
            customer_id  = str(data_req['customer_id'])
            cluster_id = str(data_req['cluster_id'])
            consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url], group_id=agent_id)
            consumer.subscribe(pattern='hivedatabasequery*')
            try:
                consumer.poll()
                my_logger.debug("Polling consumer in database consumer...")
                for message in consumer:
                    print "in first for loop"
                    consumer_data = message.value
                    #print "..........................",consumer_data
                    data = consumer_data.replace("'", '"')
                    query_data = json.loads(data)
                    print 'hellllllllllloooooooooooooooooo'
                    print query_data
                    hiveClient = HiveQuery(hive_connection, 10000,'default')
                    print "hhhhhhhhhhhhhhhhhhheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    explain_result = hiveClient.runQuery("show databases")
                    print 'starrrrrrrrrrrrrrttttttttttttt',explain_result
                    print type(explain_result)
                    names_list = []
                    database_result = {}
                    for value in explain_result['output']:
                        print str(value[0])
                        names_list.append(str(value[0]))
                    print names_list
                    key = "database_names"
                    database_result[key] = names_list
                    print database_result
                    producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
                    kafkatopic = "hivedatabaseresult_"+customer_id+"_"+cluster_id
                    kafkatopic = kafkatopic.decode('utf-8')
                    producer.send(kafkatopic, str(database_result))
                    producer.flush()
                    print "produced"
            except Exception as e:
                print e

        except Exception as e:
            print e


