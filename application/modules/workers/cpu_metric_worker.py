import json
import requests
def cpuMetrics():
	url = "http://localhost:8088/ws/v1/cluster/nodes"
	response = requests.get(url)
	result = json.loads(response.text)
	node_details = result['nodes']
	key = node_details['node']
	lis = []
	for val in key:
		host_name = val["nodeHostName"]
		metric_value = val["availableVirtualCores"]
		dict = {}
		dict["metric_name"]="cpu"
       	dict["host_name"]=str(host_name)
        dict["metric_value"]=metric_value
        lis.append(dict)
	return lis
