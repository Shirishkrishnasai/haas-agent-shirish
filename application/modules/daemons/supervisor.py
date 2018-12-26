from kafka import KafkaConsumer
from apscheduler.schedulers.background import BackgroundScheduler
from application import app,sqlite_string
import sqlite3
import os
import datetime
import subprocess
from application.configfile import agentinfo_path,kafka_server_url


def supervisoragent():
    #try:
        info = open(agentinfo_path, "r")
        content = info.read()
        data_req = json.loads(content, 'utf-8')
        agent_id = str(data_req['agent_id'])

        consumer = KafkaConsumer(bootstrap_servers=[kafka_server_url])

        consumer.subscribe(pattern='task*',group_id=agent_id)

        # Reading data from consumer and passing to the function

        for message in consumer:

            tasks_data = message.value
            # print message
            print 'hehehehehehehehehehehehe', tasks_data
            #list1.append(customer_data)
            #print list1,"list1"


            connection_to_haas = sqlite3.connect(sqlite_string)
            cur=connection_to_haas.cursor()
            cur.execute("select uid_task_id from tbl_worker_assigned_task ")
            worker_assigned_task_info=cur.fetchall()

        # Assigning tasks when there are no assigned tasks upto date


            if worker_assigned_task_info == []:
                #cur.execute("select var_agent_worker_file_name,txt_path,uid_task_id,lng_task_type_id,txt_agent_worker_version,txt_payload_id from tbl_agent_worker_task_mapping")
                #worker_tasks_info=cur.fetchall()

                #for task_information in worker_tasks_info:
                    #payloadid = task_information[5]
                    payloadid = tasks_data['payload_id']
            #Assigning to worker if worker has no arguments to be taken

                    if payloadid == None:
                        path = tasks_data['worker_path']
                        #path=task_information[1]
                        pathlist=path.split("/")
                        pythonfile_name=pathlist[-1]
                        extension_name = pythonfile_name.split(".")

                #Assigning tasks if the worker is a python file


                        if extension_name[1] == 'py':
                            task_id = tasks_data['task_id']
                            #task_id = task_information[2]
                            py_path = "'" + "python" + " " + path + "'"
                            starttime = datetime.datetime.now()
                            task_status_insert_statment = 'INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ("running",' + '"' + str(starttime) + '"' + ',"' + str(task_id) + '","f"'+')'
                            cur.execute(task_status_insert_statment)
                            connection_to_haas.commit()
                            execute = os.system(py_path)

                    #Updates status if execution is completed


                            if execute == 0:
                                endtime = datetime.datetime.now()
                                update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                cur.execute(update_task_status)
                                assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                cur.execute(assigned_task_insert_statement)
                                connection_to_haas.commit()

                #Assigning tasks if the worker is a shell script


                        else:
                            task_id = tasks_data['task_id']
                            #task_id = task_information[2]
                            sh_path = "sh" + " " + path
                            starttime = datetime.datetime.now()
                            task_status_insert_statment = 'INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ("running",' + '"' + str(starttime) + '"' + ',"' + str(task_id) + '","f")'
                            cur.execute(task_status_insert_statment)
                            connection_to_haas.commit()

                            execute = os.system(sh_path)


                    #Updates status if execution is completed


                            if execute == 0:
                                endtime = datetime.datetime.now()
                                update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                cur.execute(update_task_status)
                                assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                cur.execute(assigned_task_insert_statement)
                                connection_to_haas.commit()

            #Assigning to worker if worker has arguments to be taken


                    else:
                        task_type_id = tasks_data['task_type_id']
                        #task_type_id=task_information[3]


                        #worker_version=task_information[4]
                        payloadid = tasks_data['payload_id']
                        #payloadid=task_information[5]

                        path =tasks_data['worker_path']
                        #path=task_information[1]

                        pathlist=path.split("/")
                        pythonfile_name=pathlist[-1]
                        name = pythonfile_name.split(".")
                        py_path=[]

                #Assigning tasks if the worker is a python file


                        if name[1] == 'py':
                            task_id = tasks_data['task_id']
                            #task_id = task_information[2]

                            py_path.append("python")
                            py_path.append(path)
                            py_path.append("payload_id")
                            py_path.append(payloadid)
                            starttime = datetime.datetime.now()
                            task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                            cur.execute(task_status_insert_statment)
                            connection_to_haas.commit()
                            execute=subprocess.call(py_path, shell=False)
                            #task_id = task_information[2]

                    # Updates status if execution is completed
                            if execute == 0:
                                endtime = datetime.datetime.now()
                                update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                cur.execute(update_task_status)
                                assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                cur.execute(assigned_task_insert_statement)
                                connection_to_haas.commit()

                # Assigning tasks if the worker is a shell script


                        else:
                            task_id = tasks_data['task_id']
                            #task_id = task_information[2]

                            sh_path = []
                            sh_path.append(path)
                            sh_path.append("payload_id")
                            sh_path.append(payloadid)

                            execute = subprocess.call(sh_path, shell=True)
                            starttime = datetime.datetime.now()
                            task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                            cur.execute(task_status_insert_statment)
                            connection_to_haas.commit()

                            #task_id = task_information[2]

                    #Updates status if execution is completed


                            if execute == 0:
                                endtime = datetime.datetime.now()
                                update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                cur.execute( update_task_status)
                                assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                cur.execute(assigned_task_insert_statement)
                                connection_to_haas.commit()
        #Assigning workers if there are aready assigned worker upto date
            else:
                agent_worker_tasks=[]
                if len(worker_assigned_task_info) == 1:
                    print(output[0][0])
                    worker_tasks_info = "select var_agent_worker_file_name,txt_path,uid_task_id,lng_task_type_id,txt_agent_worker_version,txt_payload_id  from tbl_agent_worker_task_mapping where uid_task_id != '" + str(output[0][0])+"'"
                    cur.execute(worker_tasks_info)
                    worker_tasks_info = cur.fetchall()

                else:
                    for id in worker_assigned_task_info:
                        agent_worker_tasks.append(id[0])
                    tpl=tuple(agent_worker_tasks)
                    print(tpl)
                    worker_tasks_info="select var_agent_worker_file_name,txt_path,uid_task_id,lng_task_type_id,txt_agent_worker_version,txt_payload_id  from tbl_agent_worker_task_mapping where uid_task_id not in ('" +"','".join(tpl)+"')"
                    cur.execute(worker_tasks_info)
                    worker_tasks_info=cur.fetchall()
                    for task_information in worker_tasks_info:
                        payloadid = task_information[5]

            # Assigning to worker if worker has no arguments to be taken


                        if payloadid == None:
                                file_name = task_information[0]
                                path=task_information[1]
                                task_type_id = task_information[3]
                                worker_version = task_information[4]
                                print(path)
                                pathlist=path.split("/")
                                pythonfile_name=pathlist[-1]
                                extension_name = pythonfile_name.split(".")
                                task_id = task_information[2]

                #Assigning tasks if the worker is a python file


                                if extension_name[1] == 'py':
                                    py_path = "'" + "python" + " " + path + "'"
                                    starttime = datetime.datetime.now()
                                    task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,bool_flag='f',uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                                    cur.execute(task_status_insert_statment)
                                    connection_to_haas.commit()
                                    execute = os.system(py_path)

                        # Updates status if execution is completed


                                    if execute == 0:
                                        endtime = datetime.datetime.now()
                                        update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                        cur.execute(update_task_status)
                                        assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                        cur.execute(assigned_task_insert_statement)
                                        connection_to_haas.commit()

                # Assigning tasks if the worker is a shell script


                                else:
                                    sh_path = "bash" + " " + path
                                    starttime = datetime.datetime.now()
                                    task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                                    cur.execute(task_status_insert_statment)
                                    connection_to_haas.commit()
                                    execute = subprocess.call(sh_path,shell=True)

                        # Updates status if execution is completed


                                    if execute == 0:
                                        endtime = datetime.datetime.now()
                                        update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                        cur.execute(update_task_status)
                                        assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                        cur.execute(assigned_task_insert_statement)
                                        connection_to_haas.commit()

            # Assigning to worker if worker has no arguments to be taken


                        else:
                            file_name = task_information[0]
                            task_type_id = task_information[3]
                            worker_version = task_information[4]
                            payloadid = task_information[5]
                            path=task_information[1]
                            pathlist=path.split("/")
                            pythonfile_name=pathlist[-1]
                            extension_name = pythonfile_name.split(".")
                            task_id = task_information[2]

                #Assigning tasks if the worker is a python file


                            if extension_name[1] == 'py':
                                py_path = []
                                py_path.append("python")
                                py_path.append(path)
                                py_path.append("payload_id")
                                py_path.append(payloadid)
                                starttime = datetime.datetime.now()
                                task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                                cur.execute(task_status_insert_statment)
                                connection_to_haas.commit()
                                execute = subprocess.call(py_path, shell=False)

                        # Updates status if execution is completed


                                if execute == 0:
                                    endtime = datetime.datetime.now()
                                    update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                    cur.execute(update_task_status)
                                    assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                    cur.execute(assigned_task_insert_statement)
                                    connection_to_haas.commit()

                # Assigning tasks if the worker is a shell script


                            else:
                                sh_path = []
                                sh_path.append(path)

                                starttime = datetime.datetime.now()
                                task_status_insert_statment = "INSERT INTO tbl_agent_task_status (var_task_status,ts_execution_start_datetime,uid_task_id,bool_flag) VALUES ('running'," + "'" + str(starttime) + "'" + ",'" + str(task_id) + "','f')"
                                cur.execute(task_status_insert_statment)
                                connection_to_haas.commit()
                                execute = subprocess.call(sh_path, shell=True)

                        # Updates status if execution is completed


                                if execute == 0:
                                    endtime = datetime.datetime.now()
                                    update_task_status = "UPDATE  tbl_agent_task_status SET var_task_status='completed',bool_flag='f',ts_finished_datetime=" + "'" + str(endtime) + "'" + " where uid_task_id='" + str(task_id)+"'"
                                    cur.execute(update_task_status)
                                    assigned_task_insert_statement="INSERT INTO tbl_worker_assigned_task (uid_task_id) VALUES ('"+str(task_id)+"')"
                                    cur.execute(assigned_task_insert_statement)
                                    connection_to_haas.commit()

        # Sceduler job for supervisor code for ever one minute
def agentsupervisorscheduler():
                scheduler = BackgroundScheduler()
                scheduler.add_job(supervisoragent,'cron',minute='*/1' )
                scheduler.start()