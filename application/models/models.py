from sqlalchemy import  Boolean, Column, DateTime,  Integer, String,  Text,  create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base

db_url = 'sqlite:////opt/agent/haas'
engine = create_engine(db_url)


Base = declarative_base()
metadata = Base.metadata


class TblAgentTaskStatus(Base):
	__tablename__ = 'tbl_agent_task_status'

	srl_id = Column(Integer, primary_key=True)
	uid_task_id = Column(String(40))
	ts_execution_start_datetime = Column(DateTime)
	var_task_status = Column(String(50))
	txt_message = Column(Text)
	ts_finished_datetime = Column(DateTime)
	var_created_by = Column(String(20))
	var_modified_by = Column(String(20))
	ts_created_datetime = Column(DateTime)
	ts_modified_datetime = Column(DateTime)
	bool_flag = Column(Boolean)


class TblHiveQueryStatus(Base):
	__tablename__ = 'tbl_hive_query_status'

	srl_id = Column(Integer, primary_key=True)
	uid_hive_request_id = Column(String(40))
	var_query_status = Column(String(50))
	ts_status_datetime = Column(DateTime)
	bool_flag = Column(Boolean)

class TblMrJobInfo(Base):
	__tablename__ = 'tbl_mr_job_info'

	srl_id = Column(Integer,primary_key=True)
	uid_request_id = Column(String)
	var_application_id = Column(String(60))
	uid_customer_id = Column(String(60))
	uid_cluster_id = Column(String(60))
	var_job_status = Column(String(30))
	uid_conf_upload_id = Column(String(60))
	uid_jar_upload_id = Column(String(60))
	bool_job_status_produce = Column(Boolean)
	var_resourcemanager_ip = Column(String(30))
	var_job_diagnostic_status = Column(String(20))
	var_job_parameters = Column(String)
	var_file_name = Column(String)
	var_job_description = Column(String)
	var_conf_folder_name = Column(String)

class TblAgentWorkerTaskMapping(Base):
	__tablename__='tbl_agent_worker_task_mapping'

	srl_id=Column(Integer,primary_key=True)
	uid_task_id=Column(String(50))
	uid_agent_id=Column
	txt_payload_id=Column(String(40))
	lng_task_type_id=Column(String(40))
	txt_description=Column(String(40))
	txt_agent_worker_version=Column(String(50))
	var_agent_worker_file_name=Column(String(50))
	txt_path=Column(Text)
	ts_version_updated_datetime=Column(DateTime)
	bool_restart_needed_on_upgrade=Column(DateTime)
	var_created_by=Column(String(20))
	var_modified_by=Column(String(20))
	ts_created_datetime=Column(DateTime)
	ts_modified_datetime=Column(DateTime)
	var_task_status =Column(Integer)

Base.metadata.create_all(bind=engine)
ins = inspect(engine)
for _t in ins.get_table_names():
	print(_t)



