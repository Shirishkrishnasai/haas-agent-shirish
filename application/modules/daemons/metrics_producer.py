import datetime
import time
import json,os,sys
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
from application.configfile import agentinfo_path, kafka_server_url
from application.modules.workers.ram_metric_worker import ramMetrics
from application.modules.workers.cpu_metric_worker import cpuMetrics
from application.modules.workers.storage_metric_worker import storage
from application.common.loggerfile import my_logger


def kafkaMetricsProducer():
    info = open(agentinfo_path, "r")
    content = info.read()
    data_req = json.loads(content, 'utf-8')
    customer_id = str(data_req['customer_id'])
    cluster_id = str(data_req['cluster_id'])
    role=str(data_req['role'])
    info.close()
    #while True:
    try:
            if role=="namenode":
                my_logger.debug("Getting metrics and publishing....")
                producer = KafkaProducer(bootstrap_servers=[kafka_server_url])
                kafka_topic = "metrics_" + customer_id + "_" + cluster_id
                my_logger.debug(kafka_topic)
                kafkatopic = kafka_topic.decode('utf-8')
                ram_metrics = ramMetrics()
                cpu_metrics = cpuMetrics()
                storage_metrics = storage()
                metrics_list = []
                metrics_list.extend((ram_metrics,storage_metrics,cpu_metrics))
                my_logger.debug(metrics_list)
                metrics_data = {}
                metrics_data['event_type'] = "metrics"
                date_time = datetime.datetime.now()
                time_value = str(int(round(time.mktime(date_time.timetuple()))) * 1000)
                metrics_data['time'] = time_value
                metrics_data['customer_id'] = customer_id
                metrics_data['cluster_id'] = cluster_id
                metrics_data['payload'] = metrics_list
                producer.send(kafkatopic, str(metrics_data))
                producer.flush()
            else :
                pass
    except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
        #time.sleep(60)


def kafkaMetricsProducerScheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(kafkaMetricsProducer,'cron',minute='*/1' )
    scheduler.start()
    kafkaMetricsProducer()
