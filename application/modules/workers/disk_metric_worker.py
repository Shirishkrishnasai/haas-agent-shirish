import os

def disk():
        m = map(str,os.popen('iostat -dx sda').readlines()[-2].split()[1:])
        data_read= m[4]
        data_write= m[5]
        metrics = {"metric_name":"disk","disk_read":data_read,"disk_write":data_write,"measured_in":"bytes"}
        return metrics
