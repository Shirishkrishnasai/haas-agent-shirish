'''
import json
import logging
import os

import requests
from azure.storage.file import FileService
logging.basicConfig()
from application.configfile import server_url, agentinfo_path, agentregistration_connection, azure_credentials_url


def agentregisterfunc():
    try:
        print 'in agent register daemon'
        agent_data = open(agentinfo_path, "r")
        content = agent_data.read()
        print content, 'connnn'
        agent_data.close()
        data_req = json.loads(content, 'utf-8')
        cluster_id = str(data_req['cluster_id'])
        #cluster_id = 'c7ca3772-0cf4-11e9-8adb-3ca9f49ab2cc'
        print cluster_id, 'clusssss'
        customer_id = str(data_req['customer_id'])
        url = server_url + agentregistration_connection
        data = json.dumps(data_req)
        print data
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=data, headers=headers)
        print 'done'

        azure_credential_url = server_url + azure_credentials_url + customer_id
        #print azure_credential_url
        get_azure_credetials = requests.get(azure_credential_url)
        print get_azure_credetials, 'gettttttttttttt'
        dict_azure_credentials = get_azure_credetials.json()
        print dict_azure_credentials, 'dictttt'
        azure_account_name = dict_azure_credentials['account_name']
        azure_account_key = dict_azure_credentials['key']
        azure_share_name = cluster_id
        print azure_share_name, 'shareeee'
        file_service = FileService(account_name=azure_account_name, account_key=azure_account_key)

        if file_service.exists(azure_share_name) == True:
            # Script details
            print 'inside'
            script_path = "/opt/scripts"
            script_name = 'azure-mount-share.sh'
            script_arguments = azure_account_name + ' ' + azure_account_key + ' ' + azure_share_name
            execute_statement = "bash" + ' ' + script_path + '/' + script_name + ' ' + script_arguments
            # execute_statement = 'dir'
            os.system(execute_statement)
        else:
            pass
    except Exception as e:
          print e.message
'''

import json
import logging
import os

import requests
from azure.storage.file import FileService
logging.basicConfig()
from application.configfile import server_url, agentinfo_path, agentregistration_connection, azure_credentials_url


def agentregisterfunc():
    try:
        print 'in agent register daemon'
        agent_data = open(agentinfo_path, "r")
        content = agent_data.read()
        print content, 'connnn'
        agent_data.close()
        data_req = json.loads(content, 'utf-8')
        cluster_id = str(data_req['cluster_id'])
        #cluster_id = 'c7ca3772-0cf4-11e9-8adb-3ca9f49ab2cc'
        print cluster_id, 'clusssss'
        customer_id = str(data_req['customer_id'])
        url = server_url + agentregistration_connection
        data = json.dumps(data_req)
        print data
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        requests.post(url, data=data, headers=headers)
        print 'done'

        azure_credential_url = server_url + azure_credentials_url + customer_id
        #print azure_credential_url
        get_azure_credetials = requests.get(azure_credential_url)
        print get_azure_credetials, 'gettttttttttttt'
        dict_azure_credentials = get_azure_credetials.json()
        print dict_azure_credentials, 'dictttt'
        azure_account_name = dict_azure_credentials['account_name']
        azure_account_key = dict_azure_credentials['key']
        azure_share_name = cluster_id
        print azure_share_name, 'shareeee'
        file_service = FileService(account_name=azure_account_name, account_key=azure_account_key)
        state = True
        while file_service.exists(azure_share_name) == True and state == True:
        #if file_service.exists(azure_share_name) == True:
            # Script details
            print 'inside'
            script_path = "/opt/scripts"
            script_name = 'azure-mount-share.sh'
            script_arguments = azure_account_name + ' ' + azure_account_key + ' ' + azure_share_name
            execute_statement = "bash" + ' ' + script_path + '/' + script_name + ' ' + script_arguments
            # execute_statement = 'dir'
            quote = os.system(execute_statement)
            print quote,'quoteeee',type(quote)
            #state = False
            break

        else:
            pass
    except Exception as e:
         print e.message
