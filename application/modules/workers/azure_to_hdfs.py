import requests
import codecs
from azure.storage.file import FileService,FilePermissions
import base64
#Input Params - hdfs file path, azure connecction serttings, azure target file path
# output - azure file url with download id or url


chunk_size = 1024*1024*64
offset =0

# HDFS Information
hdfs_file_directory ='/sreeram/'
hdfs_file = 'sales.csv'
namenode =  'http://192.168.100.26'

hdfs_file_path = hdfs_file_directory+hdfs_file
hdfs_api_host = namenode+ ':50070'
rest_suffix = '/webhdfs/v1'

file_status_operation = '?user.name=hadoop&op=GETFILESTATUS'
read_file_operation = '?user.name=hadoop&op=OPEN'

#Azure Connection details in dictionary
azure_account_name ='sbvsolutions'
azure_account_key ='9fASv4JGbIhzM03rGFb84TuYtHVVNjfIE35N54IL4rFBOlrnqGnQTUpLPRvPHWURGqmo0QyalFtrY8fBc5JBvw=='
azure_share_name ='haasfiles'
target_directory_path_in_azure ='sreeram'
target_file_name='sales_azure.csv'

file_service = FileService(account_name=azure_account_name, account_key=azure_account_key)

file_service.

filestatus_url = hdfs_api_host+rest_suffix+hdfs_file_path+file_status_operation
print filestatus_url
file_size =  requests.get(filestatus_url).json()['FileStatus']['length']

max_iterations = int(file_size/chunk_size)+1

if (chunk_size > file_size):
    chunk_size=file_size

remaining_size = file_size
index=0
while (remaining_size >0):
    # Reading HDFS file in chunks
    file_read_url= hdfs_api_host+rest_suffix+hdfs_file_path+read_file_operation+'&off_set='+str(offset)+'&length='+str(chunk_size)+'&buffersize='+str(chunk_size)
    str_content = requests.get(file_read_url).content
    #print str_content

    #if (remaining_size)%4:
    #    str_content+='='*(4-remaining_size %4)
    #azure_write_string = (str_content).encode('base64')

    #print str_content
    # Writing File to Azure in target directory Path
    #print remaining_size

    print "Iteration -", max_iterations, offset
    print "Index",offset,"---Count",offset+chunk_size
    file_service.create_file_from_bytes(share_name=azure_share_name,
                                        directory_name=target_directory_path_in_azure,
                                        file_name=target_file_name,
                                        file=bytes(str_content),
                                        index=offset,
                                        count=chunk_size,
                                        timeout=1200,
                                        max_connections=100
                                        )


    offset = offset + chunk_size
    remaining_size = remaining_size - chunk_size
    max_iterations=max_iterations-1

    if (remaining_size <= chunk_size):
        chunk_size=remaining_size

