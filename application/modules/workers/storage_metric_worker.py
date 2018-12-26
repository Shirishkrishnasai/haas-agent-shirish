import os
import json


def storage():
        storage_details = map(str,os.popen('df  --total').readlines()[-1].split()[1:])
        available_storage_value= storage_details[2]

        storage_metrics = {"metric_name":"storage","available_storage":available_storage_value,"measured_in":"bytes"}

        return storage_metrics
