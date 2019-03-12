import subprocess
def storage():
    args="/opt/agent/application/modules/workers/hdfs1.sh"
    out_put = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = out_put.communicate()
    storage_data=str(out).replace("-------------------------------------------------","\n")
    memory_line=storage_data.split("\n\n\n")[1:]
    available_storage=[]
    for mem_data in memory_line:
       if mem_data == None:
          pass
       else:
          individual_line = str(mem_data).split("\n")
          storage_data = {}
          for element in individual_line:
              if ':' in element :
                  hdfs_remain=(element.split(':'))
                  storage_data[hdfs_remain[0]] = hdfs_remain[1]
          if storage_data != {}:
              store={}
              dfs_ram=storage_data["DFS Remaining"]
              dfs_remaining=dfs_ram.split("(")
              store["host_name"]= storage_data["Hostname"].strip()
              store["metric_value"]=dfs_remaining[0].strip()
              store["metric_name"]="storage"
              store["measured_in"]="bytes"
              available_storage.append(store)
    # print available_storage
    return available_storage
