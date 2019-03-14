import subprocess
import requests,sys,os
import json
def hdfsTextworker(hdfs_file_path,request_id):
    # print path
    # args="/home/hadoop/hdfs.sh %s" %path
    # print args
    # out_put = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell = True)
    # out, err = out_put.communicate()
    # print out,err,'lollllll'
    # response = requests.get(url="http://192.168.100.182:50070/webhdfs/v1/" + file_path + "?&op=OPEN")
    # result = response.json()
    # print result
    chunk_size = 1024 * 1024 * 64
    offset = 0

    # HDFS Information
    #hdfs_file_directory = '/hivy/'
    #hdfs_file = 'pos_file'
    namenode = 'http://192.168.100.182'

    #hdfs_file_path = hdfs_file_directory + hdfs_file
    hdfs_api_host = namenode + ':50070'

    rest_suffix = '/webhdfs/v1'
    read_file_operation = '?user.name=hadoop&op=OPEN'

    # file_read_url = hdfs_api_host + rest_suffix + hdfs_file_path + read_file_operation + '&off_set=' + str(offset)\
    #                 + '&length=' + str(chunk_size) + '&buffersize=' + str(chunk_size)
    file_read_url = hdfs_api_host + rest_suffix + hdfs_file_path + read_file_operation + '&off_set=' + str(offset) \
                    + '&length=' + str(chunk_size) + '&buffersize=' + str(chunk_size)
    print file_read_url
    str_content = requests.get(file_read_url).content
    #result = str_content.json()
    #print result
    print str_content
hdfsTextworker('/hivy/pos_file','lol')

