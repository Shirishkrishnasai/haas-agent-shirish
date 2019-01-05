import os
import json


def ramMetrics():
    metrics = map(int, os.popen('free -t -b').readlines()[-1].split()[1:])
    metric_value = metrics[1]

    ram_metrics = {"metric_name": "ram", "metric_value": metric_value, "base_value": 0, "measured_in": "bytes"}
    return ram_metrics
