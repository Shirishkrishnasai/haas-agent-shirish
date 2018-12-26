
from job_management import MapRedResourceManager

mapred = MapRedResourceManager(address="resourcemanager", port=8088)
application_id=mapred.submitJob(jar_path="/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar",filename="wordcount",filepath="/home/hadoop/mapred-site.xml",input="/sravani/file",output="/sravani/job")
print application_id