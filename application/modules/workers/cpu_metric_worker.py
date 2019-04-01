import json
import requests
def cpuMetrics():
	url = "http://localhost:8088/ws/v1/cluster/nodes"
	response = requests.get(url)
	result = json.loads(response.text)
	node_details = result['nodes']
	key = node_details['node']
	avaialable_cpu = []
	for val in key:
		host_name = val["nodeHostName"]
		metric_value = val["availableVirtualCores"]
		cpu_data = {}
		cpu_data["metric_name"]="cpu"
		cpu_data["host_name"]=str(host_name)
		cpu_data["metric_value"]=metric_value
		avaialable_cpu.append(cpu_data)
	return avaialable_cpu
