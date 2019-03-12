import json
import requests
def ramMetrics():
    url = "http://localhost:8088/ws/v1/cluster/nodes"
    response = requests.get(url)
    result = json.loads(response.text)
    node_details = result['nodes']
    key = node_details['node']
    avialable_ram = []
    for val in key:
        host_name = val["nodeHostName"]
        metric_value = val["availMemoryMB"]
        ram_data = {}
        ram_data["metric_name"]="ram"
        ram_data["host_name"]=str(host_name)
        ram_data["metric_value"]=metric_value
        ram_data["base_value"]="0"
        ram_data["measured_in"]="bytes"
        avialable_ram.append(dict)
    return avialable_ram
