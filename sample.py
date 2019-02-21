   def submitJob(address=None, port=8088, timeout=30, nn_address=None, nn_port=None, user='hadoop',location="localhost", port=5000, jar_path=None, filename=None,filepath=None,input=None, output=None,$
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

        api_endpoint = 'http://{}:{}{}/new-application'.format(address, port, '/ws/v1/cluster/apps')
        print api_endpoint\

        appid = requests.post(api_endpoint, None, None, headers=self.__getHeaders());

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
                        "value": "{{CLASSPATH}}<CPS>./*<CPS>{{HADOOP_CONF_DIR}}<CPS>{{HADOOP_COMMON_HOME}}/share/hadoop/comm$
                    },
                    {
                        "key": "DISTRIBUTEDSHELLSCRIPTLEN",
                        "value": "6"
                    }

                ]
        }
        mapRedJob = {
            "application-id": application_id,
            "application-name":application_id+"_job",
            "am-container-spec": {
                "commands": self.mapredCommand.format(jar_path,filename,filepath,input,output),
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
        print self._submitJob(jobJson=mapRedJob)
        return new_app_response['application-id']

