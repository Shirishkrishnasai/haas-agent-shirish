import json,os,sys
import requests
from application.common.loggerfile import my_logger


def ramMetrics():
    try :
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
            avialable_ram.append(ram_data)
        return avialable_ram
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
