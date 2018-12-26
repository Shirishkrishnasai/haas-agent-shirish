import os

def ramMetrics():

                tot_m, used_m, free_m = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
                ram_metrics={}

                ram_metrics['total_memory']=tot_m
                ram_metrics['used_memory']=used_m
                ram_metrics['free_memory']=free_m
                return ram_metrics

#ramMetrics()


