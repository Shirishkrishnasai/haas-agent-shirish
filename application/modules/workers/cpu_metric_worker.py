import os
import json

def cpuMetrics():

		CPU_Pct=str(round(float(os.popen('''grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage }' ''').readline()),2))

		cpu_metrics={"metric_name":"cpu" , "metric_value": CPU_Pct , "base_value":0.10, "measured_in":"%"}

		return cpu_metrics
