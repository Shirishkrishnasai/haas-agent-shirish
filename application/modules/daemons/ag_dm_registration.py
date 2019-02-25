import json
import logging
import os

import requests
from azure.storage.file import FileService
logging.basicConfig()
from application.configfile import server_url, agentinfo_path, agentregistration_connection, azure_credentials_url
from application.common.loggerfile import my_logger


def agentregisterfunc():
    try:
        my_logger.info('in agent register daemon')
        agent_data = open(agentinfo_path, "r")
        content = agent_data.read()
        my_logger.info(content)
        agent_data.close()
        data_req = json.loads(content, 'utf-8')
        cluster_id = str(data_req['cluster_id'])
        my_logger.info(cluster_id)
        customer_id = str(data_req['customer_id'])
        url = server_url + agentregistration_connection
        data = json.dumps(data_req)
        my_logger.info(data)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=data, headers=headers)
        my_logger.info('done')

        azure_credential_url = server_url + azure_credentials_url + customer_id
        #my_logger.info(azure_credential_url
        get_azure_credetials = requests.get(azure_credential_url)
        my_logger.info(get_azure_credetials)
        dict_azure_credentials = get_azure_credetials.json()
        my_logger.info(dict_azure_credentials)
        azure_account_name = dict_azure_credentials['account_name']
        azure_account_key = dict_azure_credentials['key']
        azure_share_name = cluster_id
        my_logger.info(azure_share_name)
        file_service = FileService(account_name=azure_account_name, account_key=azure_account_key)

        if file_service.exists(azure_share_name) == True:
            # Script details
            my_logger.info('inside')
            script_path = "/opt/scripts"
            script_name = 'azure-mount-share.sh'
            script_arguments = azure_account_name + ' ' + azure_account_key + ' ' + azure_share_name
            execute_statement = "bash" + ' ' + script_path + '/' + script_name + ' ' + script_arguments
            # execute_statement = 'dir'
            os.system(execute_statement)
        else:
            pass
    except Exception as e:
         my_logger.info(e.message)
