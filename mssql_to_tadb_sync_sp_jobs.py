import os
import time
import json
import General.toolbox as tb
import pyodbc
import pymysql
import pandas as pd
from db_config import db_config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from utils.Slack import RobotAnnouncement

"""
======================================================================
2025-02-25@Chalmers: Created DAG to sync data from MSSQL to MySQL
2025-03-04@Chalmers: Added ETL_type = 2, query by timestamp 
======================================================================
"""

# get variables from airflow
is_poc = Variable.get("is_poc")
is_config_table_updated = Variable.get("is_config_table_updated")
airflow_task_id = Variable.get("airflow_task_id")
crypto_key = Variable.get("crypto_key")
# slack announcement variables
channel = "your_slack_channel" # tadb channel
token = "your_slack_channel_token"


# send message to multiple slack channel
def send_msg_to_multiple_slack_channel(msg) -> None:
    
    RobotAnnouncement.PostToChannelProxy(token, proxy, channel, msg)   
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)


# before start etl process, send message to slack channel
def starting_stage() -> None:
    
    if is_poc == "1":
        etl_start_msg = f'============= (Ignore this message) Test running etl_tasks based on {etl_task_table} ============='
    else:
        etl_start_msg = f'============= start running etl_tasks based on {etl_task_table} ============='
    
    send_msg_to_multiple_slack_channel(etl_start_msg)   


# define sync config class, and add method to start/dispose source/target engine
class SyncConfig:

    def __init__(
        self,
        source_table_id: int,
        airflow_task_id: int,
        is_active: bool,
        source_username: str,
        source_password: str,
        source_host: str,
        source_db: str,
        source_table: str,
        source_sp: str,
        etl_type:int,
        arg_counts: int,
        target_username: str,
        target_password: str,
        target_host: str,
        target_db: str,
        target_table: str,
        pk_field: str,
        sync_field: str,
        max_loop_count: int,
        batch_size: int,
        id_field_name_1: str,
        last_id_sync_record_1: int,
        id_field_name_2: str ,
        last_id_sync_record_2: int,
        timestamp_field_name: str,
        last_timestamp_sync_record: str
    ):
        self.source_table_id = source_table_id
        self.airflow_task_id = airflow_task_id
        self.is_active = bool(is_active)  
        self.source_username = source_username
        self.source_password = tb.password_decode(crypto_key,source_password)
        self.source_host = source_host
        self.source_db = source_db
        self.source_table = source_table
        self.source_sp = source_sp
        self.etl_type = etl_type
        self.arg_counts = arg_counts
        self.target_username = target_username
        self.target_password = tb.password_decode(crypto_key,target_password)
        self.target_host = target_host
        self.target_db = target_db
        self.target_table = target_table
        self.pk_field = pk_field
        self.sync_field = sync_field
        self.max_loop_count = max_loop_count
        self.batch_size = batch_size
        self.id_field_name_1 = id_field_name_1
        self.last_id_sync_record_1 = last_id_sync_record_1
        self.id_field_name_2 = id_field_name_2
        self.last_id_sync_record_2 = last_id_sync_record_2
        self.timestamp_field_name = timestamp_field_name
        self.last_timestamp_sync_record = last_timestamp_sync_record


    # start source engine
    def mssql_start_source_engine(self) -> pyodbc.Connection:

        for attempt in range(3):  
            try:
                if  attempt > 0:
                    print(f"Attempt {attempt}/3: Connecting to MSSQL {self.source_host} as {self.source_username}...")
                self.source_connection = pyodbc.connect(
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.source_host};"
                    f"DATABASE={self.source_db};"
                    f"UID={self.source_username};"
                    f"PWD={self.source_password}",
                    timeout=10
                )
                
                return self.source_connection
            
            except pyodbc.Error as e:
                print(f"MSSQL connection failed (Attempt {attempt+1}/3): {e}")
                self.source_connection = None
                time.sleep(30) 

        print("All MSSQL connection attempts failed.")
        return None


    # dispose source engine
    def mssql_dispose_source_engine(self) -> None:

        if hasattr(self, "source_connection") and self.source_connection:
            try:
                self.source_connection.close()
            except Exception as e:
                print(f"MSSQL connection failed to close: {e}")
            finally:
                self.source_connection = None


    # start target engine
    def mysql_start_target_engine(self) -> pymysql.Connection:

        try:
            if is_poc == "1":
                self.target_connection = pymysql.connect(
                    host=self.target_host,
                    user=self.target_username,
                    password=self.target_password,
                    database=self.target_db
                )
            else:
                self.target_connection = pymysql.connect(
                    host=self.target_host,
                    user=self.target_username,
                    password=self.target_password,
                    database=self.target_db
                )

            return self.target_connection
        except Exception as e:
            print(f"MySQL connection failed to close: {e}")
            self.target_connection = None
            return None


    # dispose target engine
    def mysql_dispose_target_engine(self) -> None:
        if self.target_connection:
            try:
                self.target_connection.close()
            except Exception as e:
                print(f"MySQL connection failed to close: {e}")
            finally:
                self.target_connection = None


# safe sql value
def safe_sql_value(value) -> str:

    if value is None or pd.isnull(value) or value == '':
        return 'NULL'
    if isinstance(value, bytes):  
        return str(int.from_bytes(value, byteorder='big'))  
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


# check if table etl_task_record exists and update etl_task_record table
def check_and_update_etl_task_record(target_db,etl_task_table, mysql_config) -> None:

    # create target connection
    target_connection = pymysql.connect(
            host=mysql_config.host, # mysql_config.host
            # port=3307, # local
            user=mysql_config.username,
            password=tb.password_decode(crypto_key,mysql_config.password),
            db=target_db
    )
    print("Successfully connected to the MySQL database.")

    # prepare task table
    check_task_table_sql = f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
        AND table_name = '{etl_task_table}';"""

    # open target cursor
    with target_connection.cursor() as cursor: 
        cursor.execute(check_task_table_sql)
        result_task = cursor.fetchone()[0]  
        print(f'result_task:{result_task}')

        # if task table not exist, create one by airflow_task_id
        if result_task == 0:
            print("Checked: No rows returned from query")
            create_task_table_sql = f"""
            CREATE TABLE `{target_db}`.`{etl_task_table}` (
                source_table_id int NOT NULL PRIMARY KEY,
                airflow_task_id int NOT NULL, 
                is_active int DEFAULT TRUE, 
                source_username VARCHAR(50) NOT NULL, 
                source_password VARCHAR(150) NOT NULL, 
                source_host VARCHAR(50) NOT NULL,
                source_db VARCHAR(50) NOT NULL, 
                source_table VARCHAR(50) NOT NULL, 
                source_sp VARCHAR(50) NOT NULL,
                etl_type int,
                arg_counts INT NOT NULL,
                target_username VARCHAR(50) NOT NULL,
                target_password VARCHAR(150) NOT NULL, 
                target_host VARCHAR(50) NOT NULL, 
                target_db VARCHAR(50) NOT NULL,
                target_table VARCHAR(50) NOT NULL, 
                pk_field VARCHAR(50), 
                sync_field longtext NOT NULL, 
                max_loop_count INT NOT NULL,
                batch_size INT NOT NULL, 
                id_field_name_1 VARCHAR(50), 
                last_id_sync_record_1 BIGINT DEFAULT 0, 
                id_field_name_2 VARCHAR(50), 
                last_id_sync_record_2 BIGINT DEFAULT 0, 
                timestamp_field_name VARCHAR(50), 
                last_timestamp_sync_record VARBINARY(8)
                );"""

            with target_connection.cursor() as cursor:  
                cursor.execute(create_task_table_sql)
                target_connection.commit()
                print(f"New: Create new table {etl_task_table} in {target_db}")
                
    # declare variables
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # task_csv
    csv_file_path = os.path.join(current_dir, 'etl_configs', 'etl_task_record_sp.csv')
    etl_task_record_df = pd.read_csv(csv_file_path, index_col=False)    

    # use task_csv insert task 
    for _, row in etl_task_record_df.iterrows():
        insert_sql = f"""
        INSERT INTO `{target_db}`.`{etl_task_table}`
        (source_table_id, airflow_task_id, is_active, source_username, source_password, source_host, source_db, source_table, source_sp, etl_type, arg_counts,
        target_username, target_password, target_host, target_db, target_table, pk_field, sync_field, max_loop_count, 
        batch_size, id_field_name_1, last_id_sync_record_1, id_field_name_2, last_id_sync_record_2, timestamp_field_name, 
        last_timestamp_sync_record)
        VALUES (
        {safe_sql_value(row['source_table_id'])}, {safe_sql_value(row['airflow_task_id'])}, {safe_sql_value(row['is_active'])}, 
        {safe_sql_value(row['source_username'])}, {safe_sql_value(row['source_password'])}, {safe_sql_value(row['source_host'])}, {safe_sql_value(row['source_db'])}, 
        {safe_sql_value(row['source_table'])}, {safe_sql_value(row['source_sp'])}, {safe_sql_value(row['etl_type'])}, {safe_sql_value(row['arg_counts'])}, 
        {safe_sql_value(row['target_username'])}, {safe_sql_value(row['target_password'])}, {safe_sql_value(row['target_host'])}, 
        {safe_sql_value(row['target_db'])}, {safe_sql_value(row['target_table'])}, {safe_sql_value(row['pk_field'])}, {safe_sql_value(row['sync_field'])}, 
        {safe_sql_value(row['max_loop_count'])}, {safe_sql_value(row['batch_size'])}, {safe_sql_value(row['id_field_name_1'])}, 
        {safe_sql_value(row['last_id_sync_record_1'])}, {safe_sql_value(row['id_field_name_2'])}, {safe_sql_value(row['last_id_sync_record_2'])}, {safe_sql_value(row['timestamp_field_name'])}, 
        {safe_sql_value(row['last_timestamp_sync_record'])})
        ON DUPLICATE KEY UPDATE
        airflow_task_id = {safe_sql_value(row['airflow_task_id'])}, 
        is_active = {safe_sql_value(row['is_active'])}, 
        source_username = {safe_sql_value(row['source_username'])}, 
        source_password = {safe_sql_value(row['source_password'])}, 
        source_host = {safe_sql_value(row['source_host'])}, 
        source_db = {safe_sql_value(row['source_db'])}, 
        source_table = {safe_sql_value(row['source_table'])}, 
        source_sp = {safe_sql_value(row['source_sp'])}, 
        etl_type = {safe_sql_value(row['etl_type'])}, 
        arg_counts = {safe_sql_value(row['arg_counts'])}, 
        target_username = {safe_sql_value(row['target_username'])}, 
        target_password = {safe_sql_value(row['target_password'])}, 
        target_host = {safe_sql_value(row['target_host'])}, 
        target_db = {safe_sql_value(row['target_db'])}, 
        target_table = {safe_sql_value(row['target_table'])}, 
        pk_field = {safe_sql_value(row['pk_field'])}, 
        max_loop_count = {safe_sql_value(row['max_loop_count'])}, 
        batch_size = {safe_sql_value(row['batch_size'])}, 
        id_field_name_1 = {safe_sql_value(row['id_field_name_1'])}, 
        id_field_name_2 = {safe_sql_value(row['id_field_name_2'])}, 
        timestamp_field_name = {safe_sql_value(row['timestamp_field_name'])};
        """
        
        # close target cursor
        with target_connection.cursor() as cursor:  
            cursor.execute(insert_sql)
            target_connection.commit()

    # close target connection 
    if target_connection: 
            target_connection.close()

    Variable.set('is_config_table_updated', '0')


# get task from etl_task_table
def etl_tasks(airflow_task_id, target_db, etl_task_table, mysql_config) -> pd.DataFrame:
    query = f"SELECT * FROM `{target_db_config.db}`.`{etl_task_table}` WHERE is_active = 1 and  airflow_task_id = {airflow_task_id} "
    
    target_engine = pymysql.connect(
            host=mysql_config.host, # mysql_config.host
            # port=3307, # local 
            user=mysql_config.username,
            password=tb.password_decode(crypto_key,mysql_config.password),
            db=target_db
    )
    
    with target_engine.cursor() as cursor:  
        cursor.execute(query)
        tasks = cursor.fetchall()

        columns = [col[0] for col in cursor.description]  
        tasks_df = pd.DataFrame(tasks, columns=columns) 
        tasks_df.reset_index(drop=True, inplace=True)

    if target_engine:
            target_engine.close()
    
    return tasks_df


# execute stored procedure to get data from mssql
def exec_sp_to_get_data_from_mssql(conn, config, SQL_statement, params) -> pd.DataFrame:

    try:
        cursor = conn.cursor()
        cursor.execute(SQL_statement, params)
        rows = cursor.fetchall()

        if not rows:
            print("MSSQL Query executed but returned no data.")
            return pd.DataFrame()

        columns = [column[0] for column in cursor.description]

        data_dicts = []
        for row in rows:
            # convert row to dictionary
            row_dict = {col: val for col, val in zip(columns, row)}
            data_dicts.append(row_dict)

        data = pd.DataFrame(data_dicts)
        data['SourceHost'] = config.source_host
        data['SourceDB'] = config.source_db

    except Exception as e:
        print(f"MSSQL failed to query: {e}")
        return pd.DataFrame()

    return data


# generate query sql statement by config
def generate_query_sql_statement_by_config(config) -> tuple:

    # ETL type 1: query by id, do not cosider data change or update
    if config.etl_type == 1:

        # first argument is batch count, second argument is min id
        if config.arg_counts == 2:
            SQL_statement = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_{config.id_field_name_1}=?"
            return SQL_statement, (config.batch_size, config.last_id_sync_record_1)
        
        # first argument is batch count, second argument is min main_id, third argument is min sub_id
        elif config.arg_counts == 3:
            SQL_statement = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_{config.id_field_name_1}=?, @min_{config.id_field_name_2}=?"
            return SQL_statement, (config.batch_size, config.last_id_sync_record_1, config.last_id_sync_record_2)

    # ETL type 2: query by timestamp
    elif config.etl_type == 2:

        # first argument is batch count, second argument is min timestamp
        if config.arg_counts == 2:
            min_timestamp_value = config.last_timestamp_sync_record.to_bytes(8, byteorder='big')
            SQL_statement = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_SN=?"
            return SQL_statement, (config.batch_size, min_timestamp_value)

    # ETL type 4: query the entire table, but limit every batch
    elif config.etl_type == 4:

        # first argument is batch count, second argument is min id
        if config.arg_counts == 2:
            SQL_statement = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_{config.id_field_name_1}=?"
            return SQL_statement, (config.batch_size, config.last_id_sync_record_1)
        
        # first argument is batch count, second argument is min main_id, third argument is min sub_id
        elif config.arg_counts == 3:
            SQL_statement = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_{config.id_field_name_1}=?, @min_{config.id_field_name_2}=?"
            return SQL_statement, (config.batch_size, config.last_id_sync_record_1, config.last_id_sync_record_2)
        
    else:
        raise ValueError("Invalid number of etl_type & arguments in config.")


# load column rules from json file
def load_column_rules(json_file) -> dict:
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


# generate column rules
def generate_column_rules(config, json_rule) -> str:
    if json_rule["target_db"] == config.target_db and json_rule["target_table"] == config.target_table:
        return json_rule["covert_data_type"]
    return None


# convert rule for dataframe
def convert_rule_for_dataframe(dataframe, config, json_rule) -> pd.DataFrame:
    for rule in json_rule:
        column_name = rule["column_name"]
        target_type = generate_column_rules(config, rule)

        if target_type and column_name in dataframe.columns:

            if target_type == "bit":
                dataframe[column_name] = dataframe[column_name].apply(lambda x: 1 if x == True else 0)

            elif target_type == "bigint":
                dataframe[column_name] = dataframe[column_name].apply(
                    lambda x: int.from_bytes(x, byteorder="big") if isinstance(x, bytes) else x
                )

    return dataframe


# insert dataframe into tadb
def insert_dataframe_into_tadb(target_conn, dataframe, config) -> None:

    # convert NaN to None
    dataframe = dataframe.where(pd.notnull(dataframe), None)
    # convert Null to None
    dataframe = dataframe.applymap(lambda x: None if x == "Null" or pd.isnull(x) else x)
    # convert digit from str to int
    dataframe = dataframe.applymap(lambda x: int(x) if isinstance(x, str) and x.isdigit() else x)

    conn = target_conn
    if conn is None:
        print("MySQL failed to connect.")
        return

    try:
        with conn.cursor() as cursor:
            for _, row in dataframe.iterrows():
                columns = ', '.join([f"`{col}`" for col in row.index])
                placeholders = ', '.join(["%s" if col != 'inserttime' else 'NOW()' for col in row.index])
                update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in row.index])
                # print(f"columns: {columns}")
                # print(f"Values: {row}")
                insert_sql = f"""
                    INSERT INTO `{config.target_db}`.`{config.target_table}` ({columns})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """

                try:
                    cleaned_row = [None if pd.isna(value) else value for col, value in row.items() if col != 'inserttime']
                    cursor.execute(insert_sql, tuple(cleaned_row))
                except pymysql.MySQLError as e:
                    print(f"Error inserting or updating row in MySQL: {e}")
                    continue

            conn.commit()
            print("Data successfully written to MySQL.")
    except Exception as e:
        print(f"MySQL failed to query: {e}")


# update id or timestamp field value in record table
def update_id_field_value_in_record_table(target_conn, target_field, update_value, id_field_name, config) -> None:

    conn = target_conn
    if conn is None:
        print("MySQL failed to connect.")
        return

    try:
        with conn.cursor() as cursor:
            
            insert_sql = f"""
                update `{target_db_config.db}`.`{etl_task_table}`
                set {target_field} = {update_value}
                where source_table_id = {config.source_table_id};
            """

            cursor.execute(insert_sql)
            conn.commit()
            print(f"{target_field} - {id_field_name} : {update_value} updated successfully.")

    except Exception as e:
        print(f"MySQL failed to update: {repr(e)}")


# update etl task config through record table
def set_etl_task_config(row) -> SyncConfig:
    return SyncConfig(
        source_table_id=row['source_table_id'],
        airflow_task_id=row['airflow_task_id'],
        is_active=row['is_active'],
        source_username=row['source_username'],
        source_password=row['source_password'],
        source_host=row['source_host'],            
        source_db=row['source_db'],
        source_table=row['source_table'],
        source_sp=row['source_sp'],
        etl_type=row['etl_type'],
        arg_counts=row['arg_counts'],
        target_username=row['target_username'],
        target_password=row['target_password'],
        target_host=row['target_host'],
        target_db=row['target_db'],
        target_table=row['target_table'],
        pk_field=row['pk_field'],
        sync_field=row['sync_field'],
        max_loop_count=row['max_loop_count'],
        batch_size=row['batch_size'],
        id_field_name_1=None if pd.isna(row['id_field_name_1']) else row['id_field_name_1'],
        last_id_sync_record_1=row['last_id_sync_record_1'],
        id_field_name_2=None if pd.isna(row['id_field_name_2']) else row['id_field_name_2'],
        last_id_sync_record_2=row['last_id_sync_record_2'],
        timestamp_field_name=None if pd.isna(row['timestamp_field_name']) else row['timestamp_field_name'],
        last_timestamp_sync_record=None if pd.isna(row['last_timestamp_sync_record']) else int(row['last_timestamp_sync_record'])
    )


# close source and target engine, and send message to slack channel
def end_process(task_config,loop_count) -> None:
    task_config.mssql_dispose_source_engine()
    task_config.mysql_dispose_target_engine()     
    etl_pause_msg = f"Pause {task_config.source_host}.{task_config.source_db}.{task_config.source_table} sync job after {loop_count} loops. ({loop_count}/{task_config.max_loop_count})"
    send_msg_to_multiple_slack_channel(etl_pause_msg)


# main etl process
def etl_process(task_config, json_rule) -> None:

    print(f"========= Starting {task_config.source_sp} ETL process =========")

    # start source and target engine
    source_conn = task_config.mssql_start_source_engine()
    target_conn = task_config.mysql_start_target_engine()
    
    # counter and flag
    loop_count = 0
    if_no_more_result = 0

    # loop to get data from mssql and insert into mysql
    while loop_count < task_config.max_loop_count and if_no_more_result == 0 :
        
        loop_count += 1
        print(f"({loop_count}/{task_config.max_loop_count}) ETL process")
        time.sleep(2)
        dataframe = pd.DataFrame()

        # Source: MSSQL
        try:
            # generate SQL statement
            SQL_statement_query, params = generate_query_sql_statement_by_config(task_config)
            # execute stored procedure to get data from mssql
            dataframe = exec_sp_to_get_data_from_mssql(source_conn, task_config, SQL_statement_query, params)
            # convert specific data type according to the rules in the json file
            convert_rule_for_dataframe(dataframe, task_config, json_rule)

            if dataframe.empty:
                print("No data retrieved from MSSQL.")
                if_no_more_result = 1
                end_process(task_config,loop_count)
                return  

        except Exception as e:
            print(f"MSSQL ERROR: {e}")
            end_process(task_config,loop_count)       
            return  

        try:
            if not dataframe.empty:
                # Target: MySQL
                insert_dataframe_into_tadb(target_conn, dataframe, task_config)

                # update last id_field_1 value in record table
                if task_config.id_field_name_1 and task_config.id_field_name_1 != 'nan':
                    task_config.last_id_sync_record_1 = max(dataframe[task_config.id_field_name_1])     
                    if task_config.etl_type != 4:
                        update_id_field_value_in_record_table(target_conn, 'last_id_sync_record_1', task_config.last_id_sync_record_1, task_config.id_field_name_1, task_config)  

                # update last id_field_2 value in record table
                if task_config.id_field_name_2 and task_config.id_field_name_2 != 'nan':
                    task_config.last_id_sync_record_2 = max(dataframe[task_config.id_field_name_2])
                    if task_config.etl_type != 4:
                        update_id_field_value_in_record_table(target_conn, 'last_id_sync_record_2', task_config.last_id_sync_record_2, task_config.id_field_name_2, task_config)

                # update last timestamp_field value in record table
                if task_config.timestamp_field_name and task_config.timestamp_field_name != 'nan':
                    task_config.last_timestamp_sync_record = int(max(dataframe[task_config.timestamp_field_name]))
                    update_id_field_value_in_record_table(target_conn, 'last_timestamp_sync_record', task_config.last_timestamp_sync_record, task_config.timestamp_field_name, task_config)

        except Exception as e:
            print(f"MySQL ERROR: {repr(e)}")

    # send message to slack channel
    end_process(task_config,loop_count)


# get db config by environment
if is_poc == '1':
    target_db_config = db_config("POC_MySQL_account")  
else:
    target_db_config = db_config("PROD_MySQL_account")

# declare variables
proxy = target_db_config.proxy
record_target_db = target_db_config.db
etl_task_table = 'etl_task_record_sp'
current_dir = os.path.dirname(os.path.abspath(__file__))
json_file_path = os.path.join(current_dir, 'etl_configs', 'convert_rule.json')
json_rule = load_column_rules(json_file_path)

# check and update etl task record table if is_config_table_updated is 1
if is_config_table_updated == '1':
    check_and_update_etl_task_record(record_target_db,etl_task_table, target_db_config)

# get tasks from etl_task_table
tasks_df = etl_tasks(airflow_task_id, record_target_db, etl_task_table, target_db_config)
tasks_df.replace(['nan', 'NULL'], pd.NA, inplace=True)

# define DAG
with DAG(
    "mssql_to_mysql_sync_tasks",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description="Daily tasks for synchronizing tables from MSSQL to MySQL",
    schedule_interval='0 7 * * *',  # UTC+0
    catchup=False,
    tags=["mssql", "mysql", "etl"],
    
) as dag:

    # send message to slack channel before starting etl process
    starting_stage_op = PythonOperator(
            task_id=f"starting_stage",
            python_callable=starting_stage,
            trigger_rule=TriggerRule.ALL_DONE
        )

    previous_task = None

    # loop through tasks_df to create etl process
    for index, row in tasks_df.iterrows():
        
        # set etl task config
        task_config = set_etl_task_config(row)
        # set task_id_prefix
        task_id_prefix = f"{task_config.source_table_id}_{task_config.source_db}_{task_config.source_sp}"

        # create main etl process task
        process_table_op = PythonOperator(
            task_id=f"{task_id_prefix}_process",
            python_callable=etl_process,
            op_kwargs={
                'task_config': task_config,
                'json_rule': json_rule,
            },
            trigger_rule=TriggerRule.ALL_DONE
        )

        if previous_task:
            previous_task >> process_table_op
        else:    
            starting_stage_op

        previous_task = process_table_op