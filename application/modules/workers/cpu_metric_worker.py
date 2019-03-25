import json,os,sys
import requests
from application.common.loggerfile import my_logger


def cpuMetrics():
	try:
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
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		my_logger.error(exc_type)
		my_logger.error(fname)
		my_logger.error(exc_tb.tb_lineno)

