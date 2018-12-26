import datetime
import time
import json
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from application.configfile import agentinfo_path1,kafka_server_url
from application.modules.workers.ram_metric_worker import ramMetrics
from application.modules.workers.cpu_metric_worker import cpuMetrics
from application.modules.workers.network_metric_worker import network
from application.modules.workers.storage_metric_worker import storage
from application.modules.workers.disk_metric_worker import disk
from application.common.metrics_updation import metricSubscriber

def kafkaMetricsProducer1():
    try:

        info = open(agentinfo_path1, "r")
        content = info.read()
        data_req = json.loads(content, 'utf-8')
        customer_id = str(data_req['customer_id'])
        cluster_id = str(data_req['cluster_id'])
        agent_id = str(data_req['agent_id'])

        producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
        kafka_topic="metrics_"+customer_id+"_"+cluster_id
        print kafka_topic
        kafkatopic = kafka_topic.decode('utf-8')
        ram_metrics = ramMetrics()
        cpu_metrics = cpuMetrics()
        network_metrics = network()
        storage_metrics = storage()
        disk_metrics = disk()
        metrics_list = []
        metrics_list.extend((ram_metrics, cpu_metrics, network_metrics,storage_metrics,disk_metrics))
        metrics_data={}
        metrics_data['event_type']="metrics"
        date_time = datetime.datetime.now()

        time_value = str(int(round(time.mktime(date_time.timetuple()))))

        metrics_data['time']=time_value
        metrics_data['customer_id']=customer_id
        metrics_data['cluster_id']=cluster_id
        metrics_data['agent_id']=agent_id
        metrics_data['payload']=metrics_list
        print metrics_data
        metricSubscriber(data=metrics_data)
        producer.send(kafkatopic, str(metrics_data))
        producer.flush()
    except Exception as e:
        return e


def kafkaMetricsProducerScheduler1():
    scheduler = BackgroundScheduler()
    scheduler.add_job(kafkaMetricsProducer1,'cron',minute='*/1' )
    scheduler.start()