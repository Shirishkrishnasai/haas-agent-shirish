from application.common.loggerfile import my_logger
from pyhive import hive
from TCLIService.ttypes import TOperationState
import os,sys

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
                cursor = conn.cursor()
                # print "tilllll hereeeeeeeeeee doneeeeeeeee"
                cursor.execute(query, async=True)

                # print "this is not doneeeeeeeee what tooooooooooo dooooooooooo"
                status = cursor.poll().operationState
                # print "the initial status",status
                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    status = cursor.poll().operationState

                result_dict={}
                result_dict['status'] = status
                result_dict['description'] = cursor.description
                result_dict['output'] = cursor.fetchall()
                return result_dict

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            my_logger.error(exc_type)
            my_logger.error(fname)
            my_logger.error(exc_tb.tb_lineno)
            return e.__str__()


    def runQuery(self, query):
        return self.__run_query(query , self.__create_connection())


    def __run_noresult_query(self, query="", conn=None):
        try:
            if conn:

                cursor = conn.cursor()
                cursor.execute(query, async=True)
                status = cursor.poll().operationState
                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    status = cursor.poll().operationState
                return status
        except Exception as e:
            return e.__str__()

    def runNoResultQuery(self, query):
        return self.__run_noresult_query(query , self.__create_connection())

