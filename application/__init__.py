from requests.exceptions import HTTPError
from flask import Flask, url_for
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['Access-Control-Allow-Origin'] = '*'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////opt/agent/haas'
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


db = SQLAlchemy(app)

engine = create_engine("sqlite:////opt/agent/haas")
session_factory = sessionmaker(bind=engine)
sqlite_string= "/opt/agent/haas"
from application.modules.daemons.spark_job_consumer_daemon import sparkjobinsertionscheduler
from application.modules.daemons.spark_job_status_daemon import sparkjobstatusscheduler
from application.modules.daemons.spark_job_diagnostics_daemon import sparkjobdiagnosticscheduler

from application.modules.daemons.hive_work_status import hive_work_status_schduler
from application.modules.daemons.ag_dm_registration import agentregisterfunc
from application.modules.daemons.ag_dm_task_monitor import agentmonitorscheduler
from application.modules.daemons.metrics_producer import kafkaMetricsProducerScheduler
from application.modules.daemons.agent_daemon import agentdaemonscheduler
from application.modules.daemons.ag_dm_registration import agentregisterfunc
from application.modules.daemons.ag_dm_task_monitor import agentmonitorscheduler
from application.modules.daemons.metrics_producer import kafkaMetricsProducerScheduler
from application.modules.daemons.supervisor import supervisorcheduler
from application.modules.daemons.hive_query_consumer import hiveQueryConsumerScheduler
from application.modules.daemons.hive_status_producer import hiveStatusScheduler
from application.modules.daemons.hive_work_status import hive_work_status_schduler
from application.modules.daemons.hive_database_query_consumer import hiveDatabaseQueryConsumer
from application.modules.daemons.job_diagnostics_producer import jobdiagnosticsscheduler
from application.modules.daemons.job_status_producer import jobstatusscheduler
from application.modules.daemons.job_insertion import jobinsertionscheduler
from configfile import hive_connection
from application.modules.daemons.hdfs_browser_daemon import hdfsBrowserDaemonScheduler

hivyc = hive_connection
from application.modules.daemons.job_diagnostics_producer import jobdiagnostics
from application.modules.daemons.job_status_producer import jobstatus
from application.modules.daemons.job_insertion import insertjob
from multiprocessing import Process

def has_no_empty_params(rule):
    defaults = rule.defaults if rule.defaults is not None else ()
    arguments = rule.arguments if rule.arguments is not None else ()
    return len(defaults) >= len(arguments)
@app.route("/site-map")
def site_map():
    links = []
    for rule in app.url_map.iter_rules():
        # Filter out rules we can't navigate to in a browser
        # and rules that require parameters
        if "GET" in rule.methods and has_no_empty_params(rule):
            url = url_for(rule.endpoint, **(rule.defaults or {}))
            links.append((url, rule.endpoint))
    # links is now a list of url, endpoint tuples
    print (links)

kafkaMetricsProducerScheduler()
agentregisterfunc()
agentdaemonscheduler()
supervisorcheduler()
agentmonitorscheduler()
hiveQueryConsumerScheduler()
jobstatusscheduler()
jobinsertionscheduler()
jobdiagnosticsscheduler()
sparkjobinsertionscheduler()
sparkjobstatusscheduler()
sparkjobdiagnosticscheduler()
hdfsBrowserDaemonScheduler()
hive_work_status_schduler()
print __name__,"Running..."

def runProcess():
    """ add More processs here to run parallelly"""
#    jobinsertion_process=Process(target=jobinsertionscheduler)
#    jobinsertion_process.start()
#    jobdiagnostics_process=Process(target=jobdiagnosticsscheduler)
#    jobdiagnostics_process.start()
#    jobstatus_process=Process(target=jobstatusscheduler)
 #   jobstatus_process.start()

    #filebrowsing_process=Process(target=webhdfs)
    #filebrowsing_process.start()`
    #kafkaMetricsProducerScheduler_Process=Process(target=kafkaMetricsProducerScheduler)
    #kafkaMetricsProducerScheduler_Process.start()
    #supervisoragent_Process=Process(target=supervisorcheduler)
    #supervisoragent_Process.start()
    #agentmonitorscheduler_Process=Process(target=agentmonitorscheduler)
    #agentmonitorscheduler_Process.start()
    #agentdaemonscheduler_Process = Process(target=agentdaemonscheduler)
    #agentdaemonscheduler_Process.start()

    #hiveQueryConsumer_Process = Process(target=hiveQueryConsumerScheduler())
    #hiveQueryConsumer_Process.start()

    #job_diagnostics_producer_Process = Process(target=jobdiagnostics)
    #job_diagnostics_producer_Process.start()
    #job_status_producer_Process = Process(target=jobstatus)
    #job_status_producer_Process.start()

    #hiveDatabaseQueryConsumer_Process = Process(target=hiveDatabaseQueryConsumer)
    #hiveDatabaseQueryConsumer_Process.start()
    #hiveStatusScheduler_Process = Process(target=hiveStatusScheduler)
    #hiveStatusScheduler_Process.start()
    print "running Processs....",__name__,"running process"
#if __name__ == "application":
#    print "running process again",__name__
#    runProcess()


#hiveExplain()
