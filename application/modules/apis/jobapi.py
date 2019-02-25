import requests
import json, time
from flask import Blueprint

mapredjob=Blueprint("mapredjob",__name__)
@mapredjob.route('/job',methods=['GET'])




def submitJob(address='192.168.100.169',location="localhost", port=5000, jar_path="/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.1.jar", filename="wordcount",filepath="/tmp/job_1549366992927/mrjob.jar",input="/input/file", output="/test/map_0307",  type="mapr", **kwargs):
        """
        :param location: to where submit job
        :param port:   port of server
        :param jar_path:   jar file path
        :param input:  input dir
        :param output:  ouput dir
        :param type:   type of job mapr | spark
        :param kwargs:  additional if any
        :return:  application/job id

        """
#	geturl='http://{}:{}{}?user_name=hadoop'.format(address,port,'')
        api_endpoint = 'http://{}:{}{}/new-application?user_name=hadoop'.format(address, port,'')
        print api_endpoint\

        appid = requests.post(api_endpoint, None, None, headers = {"Content-type": "application/json"});

        new_app_response = json.loads(appid.content)
        application_id = new_app_response['application-id']
	print new_app_response
        resources = {
            "memory": (1024 if not kwargs.get("memory") else kwargs.get("memory")),
            "vCores": (1 if not kwargs.get("vcores") else kwargs.get("vcores"))
        }
        print resources
        environment = {
            "entry":
                [
                    {
                        "key": "DISTRIBUTEDSHELLSCRIPTTIMESTAMP",
                        "value": "1405459400754"
                    },

                    {
                        "key": "CLASSPATH",
                        "value": "{{CLASSPATH}}<CPS>./*<CPS>{{HADOOP_CONF_DIR}}<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/*<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/common/lib/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/*<CPS>{{HADOOP_HDFS_HOME}}/share/hadoop/hdfs/lib/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/*<CPS>{{HADOOP_YARN_HOME}}/share/hadoop/yarn/lib/*<CPS>./log4j.properties"
                    },
                    {
                        "key": "DISTRIBUTEDSHELLSCRIPTLEN",
                        "value": "6"
                    }

                ]
        }
	mapredCommand = "hadoop jar {} {} -D {} {} {}"
        mapRedJob = {
            "application-id": application_id,
            "application-name":application_id+"_job",
            "am-container-spec": {
                "commands": mapredCommand.format(jar_path,filename,filepath,input,output),
                "environment":environment
            },
            "unmanaged-AM": 'false',
            "max-app-attempts": 2,
            "resource": {
                "memory": (1024 if not kwargs.get("memory") else kwargs.get("memory")),
                "vCores": (1 if not kwargs.get("vcores") else kwargs.get("vcores"))
            },
            "application-type": "MAPREDUCE",
            "keep-containers-across-application-attempts": 'false'
        }
        print (mapRedJob)
        time.sleep(10)
        return new_app_response['application-id']

