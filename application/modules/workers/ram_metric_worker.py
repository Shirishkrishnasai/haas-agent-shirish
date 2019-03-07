import json
import requests
def ramMetrics():
    url = "http://localhost:8088/ws/v1/cluster/nodes"
    response = requests.get(url)
    result = json.loads(response.text)
    node_details = result['nodes']
    key = node_details['node']
    tuple = []
    for val in key:
        host_name = val["nodeHostName"]
        metric_value = val["availMemoryMB"]
        dict = {}
        dict["metric_name"]="ram"
        dict["host_name"]=str(host_name)
        dict["metric_value"]=metric_value
        dict["base_value"]="0"
        dict["measured_in"]="bytes"
        tuple.append(dict)
    return tuple
