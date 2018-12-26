from pyhive import hive
from TCLIService.ttypes import TOperationState
import  logging
import time

class HiveQuery:

    def __init__(self, host=None, port=None, database=None):
        self.host = "localhost" if host is None else host
        self.port = 10000 if port is None else port
        self.database = "default" if database is None else database

    def __create_connection(self):
        return hive.connect(self.host, self.port, database=self.database,username="hadoop")
    """
    :returns: columns: tuple
    :returns: data :tuple
    """
    def __run_query(self, query="", conn=None):
        try:
            if conn:
                messages=[]
                cursor = conn.cursor()
                print "tilllll hereeeeeeeeeee doneeeeeeeee"
                cursor.execute(query, async=True)

                print "this is not doneeeeeeeee what tooooooooooo dooooooooooo"
                status = cursor.poll().operationState
                print "the initial status",status
                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    print "the status you wanted",status
                    #logs = cursor.fetch_logs()
                    #for message in logs:
                     #   messages.append(message)
                    #print "class messagessssssssssssssssssss",messages
                    #time.sleep(2)
                    status = cursor.poll().operationState





                print "now this is hive query status",status


                if status==TOperationState.FINISHED_STATE:
                    print 'result existsssssssssssssssssss'
                else:
                    print 'noooooooooo resulttttttttt setttttttttttt'




                result_dict={}
                result_dict['status'] = status
                result_dict['description'] = cursor.description
                result_dict['output'] = cursor.fetchall()
                return result_dict

        except Exception as e:
            print e.message
            return e.__str__()


    def runQuery(self, query):
        return self.__run_query(query , self.__create_connection())


    def __run_noresult_query(self, query="", conn=None):
        try:
            if conn:

                cursor = conn.cursor()
                cursor.execute(query, async=True)
                status = cursor.poll().operationState
                print "the initial status",status
                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    #print "the status you wanted",status
                    #logs = cursor.fetch_logs()
                    #for message in logs:
                     #   messages.append(message)
                    #print "class messagessssssssssssssssssss",messages
                    #time.sleep(2)
                    status = cursor.poll().operationState
                return status
        except Exception as e:
            return e.__str__()

    def runNoResultQuery(self, query):
        return self.__run_noresult_query(query , self.__create_connection())

