
from job_management import MapRedResourceManager
from application.common.loggerfile import my_logger

mapred = MapRedResourceManager(address="resourcemanager", port=8088)
application_id=mapred.submitJob(jar_path="/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar",filename="wordcount",filepath="/home/hadoop/mapred-site.xml",input="/sravani/file",output="/sravani/job")
my_logger.info(application_id)