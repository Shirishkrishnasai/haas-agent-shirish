import os
import time
def network():
        #m = map(str,os.popen('sudo speedtest-cli').readlines()[-3].split()[1:])
        #data_out= m[0]
        #n = map(str,os.popen('sudo speedtest-cli').readlines()[-1].split()[1:])
        #data_in= n[0]
        data_in = 456785
        data_out = 5667
        metrics = {"metric_name":"network","data_in":data_in,"data_out":data_out,"measured_in":"bytes"}
        return  metrics
network()
