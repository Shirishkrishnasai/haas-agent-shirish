import requests
# from yarn_api_client import ApplicationMaster, HistoryServer, NodeManager, ResourceManager
import json, time


class HDFSManager():
    def __init__(self, address=None, port=500070, version="v1", user="hadoop"):
        self.address = address
        self.version = version
        self.port = port
        self.username = user

    def __makeGet(self, url="", data=None, json_data=None):

        print "calling ", url
        resp = requests.get(url, headers={"Content-type": "application/json"})
        data = json.loads(resp.content)
        return data

    def __makePost(self, url="", data=None, json_data=None, files=None):
        print "calling ", url
        resp = requests.put(url, files=files, headers={"Content-type": "application/json"})
        print resp.reason
        if (resp.status_code >= 200 and resp.status_code < 300):
            if resp.content:
                data = json.loads(resp.content)
                return data
        else:
            return {"error": 'yes',
                    "message": "an error occured with response code {} and with message".format(resp.status_code,
                                                                                                resp.content)}

    def listFiles(self, path="/"):
        return self.__makeGet(self.getApiUrl(path, "LISTSTATUS"))

    def listDirectories(self, path="/", recusrive=False):
        """
        Not Working recursively
        :param path:
        :param recusrive:
        :return:
        """
        return self.__makeGet(
            self.getApiUrl(path, ("LISTSTATUS" if not recusrive else "&op=LISTSTATUS_BATCH&startAfter=bazfile")))

    def getHomeDirectory(self):
        return self.__makeGet(self.getApiUrl("/", "GETHOMEDIRECTORY"))

    def getContentSummary(self, path="/"):
        return self.__makeGet(self.getApiUrl(path, "GETCONTENTSUMMARY"))

    def getApiUrl(self, url="", op=""):
        return 'http://{}:{}/webhdfs/{}{}?user.name={}&&op={}'.format(self.address,
                                                                      self.port,
                                                                      self.version,
                                                                      url,
                                                                      self.username,
                                                                      op)

    def copyFromLocal(self, file=None, path=None):
        if file is None or path is None:
            raise Exception("local file path adn hadoop path are mandatory to copy")
        """
             :param file: Local file path
              :param path:   hdfsPath in nothing specified default to home directory
              :return: hdfspath
              """
        self.__makePost(
            url=self.getApiUrl("/home/hadoop/" if path is None else path, "CREATE&overwrite=true"),
            files={'file': file if file is not None else "/"})
        return path


class MapRedResourceManager():

    def __init__(self, address=None, port=8088, timeout=30, nn_address=None, nn_port=None, user="hadoop"):
        self.address = address
        self.port = port
        self.timeout = timeout
        self.nn_address = nn_address
        self.username = user
        self.nn_port = nn_port
        self.maxJvm = 512
        self.mapredCommand = "hadoop jar {} {} -D {} {} {}"
    def _submitJob(self, jobJson=None):
        return self.__makePost(self.getApiUrl(self.__getAppUrl()), json_data=jobJson)

    def submitJob(self, location="localhost", port=None, jar_path=None, filename=None,filepath=None,input=None, output=None,  type="mapr", **kwargs):
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

        api_endpoint = 'http://{}:{}{}/new-application'.format(self.address, self.port, self.__getAppUrl())
        print api_endpoint\

        appid = requests.post(api_endpoint, None, None, headers=self.__getHeaders());

        new_app_response = json.loads(appid.content)
        application_id = new_app_response['application-id']
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
        mapRedJob = {
            "application-id": application_id,
            "application-name": application_id + "_job",
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

    def prepareParams(self):
        pass

    def getApiUrl(self, url=""):
        return 'http://{}:{}{}?user_name=hadoop'.format(self.address, self.port, url)

    def __getHeaders(self):
        headers = {"Content-type": "application/json"}

        return headers

    def monitorJob(self, application_id=None):
        pass

    def __submitJob(self):
        pass

    def __getAppUrl(self):
        return "/ws/v1/cluster/apps"

    def __monitorUrl(self):
        return ""

    """
    // get application ID
    // Upload Jar to hdfs 
    // prepare configuration paramters to run
    // Submit job
    // Send job status to consumer
    // Monitor Job
    """

    def __makePost(self, url="", data=None, json_data=None ,params=None):
        print "calling ", url
        resp = requests.post(url, data, json_data, headers={"Content-type": "application/json"})
        if (resp.status_code >= 200 and resp.status_code < 300):
            if resp.content:
                data = json.loads(resp.content)
                return data
        else:
            return {"error": 'yes',
                    "message": "an error occured with response code {} and with message".format(resp.status_code,
                                                                                                resp.content)}

    def __makeGet(self, url="", data=None, json_data=None):
        print "calling ", url
        resp = requests.get(url, headers={"Content-type": "application/json"})
        data = json.loads(resp.content)
        return data

    def getClusterInfo(self):
        return self.__makeGet(url=self.getApiUrl("/ws/v1/cluster/info"))

    def getCluster(self):
        return self.__makeGet(url=self.getApiUrl("/ws/v1/cluster"))

    def getApplicationDetails(self, application_id):
        return self.__makeGet(url=self.getApiUrl("/ws/v1/cluster/apps/" + application_id))

    def getApplications(self):
        return self.__makeGet(url=self.getApiUrl("/ws/v1/cluster/apps"))
