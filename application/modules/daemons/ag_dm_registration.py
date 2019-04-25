import json
import logging
import os,sys
import requests
from application.common.loggerfile import my_logger
from azure.storage.file import FileService
logging.basicConfig()
from application.configfile import server_url, agentinfo_path, agentregistration_connection, azure_credentials_url
def agentregisterfunc():
    try:
        agent_data = open(agentinfo_path, "r")
        content = agent_data.read()
        agent_data.close()
        data_req = json.loads(content, 'utf-8')
        cluster_id = str(data_req['cluster_id'])
        customer_id = str(data_req['customer_id'])
        url = server_url + agentregistration_connection
        data = json.dumps(data_req)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=data, headers=headers)
        azure_credential_url = server_url + azure_credentials_url + customer_id
        get_azure_credetials = requests.get(azure_credential_url)
        dict_azure_credentials = get_azure_credetials.json()
        azure_account_name = dict_azure_credentials['account_name']
        azure_account_key = dict_azure_credentials['key']
        azure_share_name = cluster_id
        file_service = FileService(account_name=azure_account_name, account_key=azure_account_key)

        if file_service.exists(azure_share_name) == True:
            script_path = "/opt/scripts"
            script_name = 'azure-mount-share.sh'
            script_arguments = azure_account_name + ' ' + azure_account_key + ' ' + azure_share_name
            execute_statement = "bash" + ' ' + script_path + '/' + script_name + ' ' + script_arguments
            os.system(execute_statement)
            script="/host.sh"
            command_execute="sh " +script_path+script
            print command_execute
            os.system(command_execute)
        else:
            pass
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        my_logger.error(str(e))
        my_logger.error(exc_type)
        my_logger.error(fname)
        my_logger.error(exc_tb.tb_lineno)
