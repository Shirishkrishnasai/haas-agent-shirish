import subprocess
def storage():
        p = subprocess.Popen(["hdfs", "dfsadmin", "-report"], stdout=subprocess.PIPE)
        stdout = p.communicate()
        stdout=str(stdout).replace('-------------------------------------------------','\n')
        stdout = str(stdout).split("\\n\\n\\n")[0:]
        lis=[]
        for data in stdout:
            if data == None:
                 pass
            else:
                value = data.split("\\n")
                dicto = {}
                for valu in value:
                    if ':' in valu :
                       va=(valu.split(':'))
                       dicto[va[0]] = va[1]
                if dicto != {}:
                        dic={}
                        dfs_ram=dicto["DFS Remaining"]
                        dfs_remaining=dfs_ram.split("(")
                        dic["host_name"]= dicto["Hostname"].strip()
                        dic["metric_value"]=dfs_remaining[0]
                        dic["metric_name"]="storage"
                        dic["measured_in"]="bytes"
                        lis.append(dic)
	    return lis
