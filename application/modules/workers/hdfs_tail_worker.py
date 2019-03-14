import subprocess
def hdfsTailworker(path,request_id):
    print path
    args="/opt/agent/application/hdfs_tail.sh %s" %path
    print args
    out_put = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
    out, err = out_put.communicate()
    print out,err,'lollllll'
#hdfsTailworker('/sri/pos_file')




