import os
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from utils.Slack import RobotAnnouncement
from AppSettings import getAppSettingsObj
import pymysql
import zlib
import json
import time
from datetime import datetime, timezone, timedelta
from db_config import db_config
import general.toolbox as tb

# Chalmers 2024-12-04 20:59 commit

# environment var related to connection string
# ======================================================================
appSetting = getAppSettingsObj() # get proxy name
# proxy = None
proxy = appSetting.http_proxy

# slack announcement var
# ======================================================================
channel = "your_slack_channel" 
token = "your_slack_channel_token"


def send_msg_to_multiple_slack_channel(msg):
    
    RobotAnnouncement.PostToChannelProxy(token, proxy, channel, msg)   
    os.environ.pop("http_proxy", None)
    os.environ.pop("https_proxy", None)


class etl_task_connection_config:
    
    def __init__(self, source_username, source_password, source_host, source_db, source_table, target_username, target_password, target_host, target_db, target_table, pk_field, sync_field, etl_type, max_loop_count, batch_size, partition_size, id_field_name, last_id_sync_record, last_id_house_keeping_record, timestamp_field_name, last_timestamp_sync_record, last_timestamp_house_keeping_record, source_write_username, source_write_password, if_need_house_keeping):
        self.source_username = source_username
        self.source_password = tb.password_decode(crypto_key, source_password)
        self.source_host = source_host
        self.source_db = source_db
        self.source_table = source_table
        self.source_engine = None
        self.target_username = target_username
        self.target_password = tb.password_decode(crypto_key, target_password)
        self.target_host = target_host
        self.target_db = target_db
        self.target_table = target_table
        self.target_engine = None
        self.pk_field = pk_field
        self.sync_field = sync_field
        self.etl_type = etl_type
        self.max_loop_count = max_loop_count
        self.batch_size = batch_size
        self.partition_size = partition_size
        self.id_field_name = id_field_name
        self.last_id_sync_record = last_id_sync_record
        self.last_id_house_keeping_record = last_id_house_keeping_record
        self.timestamp_field_name = timestamp_field_name
        self.last_timestamp_sync_record = last_timestamp_sync_record
        self.last_timestamp_house_keeping_record = last_timestamp_house_keeping_record
        self.source_write_username = source_write_username
        self.source_write_password = tb.password_decode(crypto_key, source_write_password)
        self.if_need_house_keeping = if_need_house_keeping        
       
    def start_source_engine(self):
        #
        self.source_connection = pymysql.connect(
            host=self.source_host,
            user=self.source_username,
            password=self.source_password,  
            database=self.source_db,
            # port=3307 # for POC
        )
        return self.source_connection

    def dispose_source_engine(self):
        #
        if self.source_connection:
            self.source_connection.close()
            
    def start_source_house_keeping_engine(self):
        #
        self.source_connection = pymysql.connect(
            host=self.source_host,
            user=self.source_write_username,
            password=self.source_write_password,  
            database=self.source_db,
            # port=3307 # for POC
        )
        return self.source_connection

    def dispose_source_house_keeping_engine(self):
        #
        if self.source_connection:
            self.source_connection.close()

    def start_target_engine(self):
        #
        self.target_connection = pymysql.connect(
            host=self.target_host,
            user=self.target_username,
            password=self.target_password, 
            database=self.target_db,
            # port=3307 # for POC
        )
        return self.target_connection

    def dispose_target_engine(self):
        #
        if self.target_connection:
            self.target_connection.close()


def create_etl_task_connection_config(row):
    return etl_task_connection_config(
        source_username=row['source_username'],
        source_password=row['source_password'],
        source_host=row['source_host'],            
        source_db=row['source_db'],
        source_table=row['source_table'],
        target_username=row['target_username'],
        target_password=row['target_password'],
        target_host=row['target_host'],
        target_db=row['target_db'],
        target_table=row['target_table'],
        pk_field=row['pk_field'],
        sync_field=row['sync_field'],
        etl_type=row['etl_type'],
        max_loop_count=row['max_loop_count'],
        batch_size=row['batch_size'],
        partition_size=row['partition_size'],
        id_field_name=row['id_field_name'],
        last_id_sync_record=row['last_id_sync_record'],
        last_id_house_keeping_record=row['last_id_house_keeping_record'],
        timestamp_field_name=row['timestamp_field_name'],
        last_timestamp_sync_record=row['last_timestamp_sync_record'],
        last_timestamp_house_keeping_record=row['last_timestamp_house_keeping_record'],
        source_write_username=row['source_write_username'],
        source_write_password=row['source_write_password'],
        if_need_house_keeping=row['if_need_house_keeping']
    )


def etl_tasks(airflow_task_id, etl_task_table, db_connection_config):
    query = f"""SELECT * FROM `{initial_etl_task_connection_config.target_db}`.`{etl_task_table}` 
        WHERE is_active = 1 and  airflow_task_id = {airflow_task_id} and etl_type not in (0,4) and if_need_house_keeping = 1
        """
    
    target_engine = db_connection_config.start_target_engine()
    
    with target_engine.cursor() as cursor:  
        cursor.execute(query)
        tasks = cursor.fetchall()

        columns = [col[0] for col in cursor.description]  
        tasks_df = pd.DataFrame(tasks, columns=columns) 
        tasks_df.reset_index(drop=True, inplace=True)

    db_connection_config.dispose_target_engine()  
    
    return tasks_df

def starting_stage():
    #
    if is_poc:
        etl_start_msg = f'============= (Ignore) start testing house_keeping_tasks on mysql ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   
    else:
        etl_start_msg = f'============= start running house_keeping_tasks on mysql ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   
            

def execute_sql(db_connection, query, db_connection_config, if_return=False):
    if db_connection == 'source':
        connection = db_connection_config.start_source_engine()
    elif db_connection == 'source_house_keeping':
        connection = db_connection_config.start_source_house_keeping_engine()        
    elif db_connection == 'target':
        connection = db_connection_config.start_target_engine()

    if if_return:
        df = pd.read_sql(query, connection)
        df.reset_index(drop=True, inplace=True)
        if db_connection == 'source':
            db_connection_config.dispose_source_engine()
        elif db_connection == 'source_house_keeping':
            db_connection_config.dispose_source_house_keeping_engine()            
        elif db_connection == 'target':    
            db_connection_config.dispose_target_engine()
        return df
    else:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()  
        if db_connection == 'source':
            db_connection_config.dispose_source_engine()
        elif db_connection == 'source_house_keeping':
            db_connection_config.dispose_source_house_keeping_engine()            
        elif db_connection == 'target':    
            db_connection_config.dispose_target_engine()
        return True
    

def get_ids_compress_list_from_etl_sync_ids (db_connection_config):
    
    delete_sql = f"""
                delete from {initial_etl_task_connection_config.target_db}.{etl_sync_ids}
                where 1=1
                and is_source_delete = 1
                and date(etl_update_date) < DATE_sub(CURRENT_DATE(), interval 14 day ); 
                """
    
    query = f"""
                select house_keeping_id, ids_compress
                from {initial_etl_task_connection_config.target_db}.{etl_sync_ids}
                where 1=1
                and date(etl_update_date) >= DATE_sub(CURRENT_DATE(), interval {date_sub_start_day} day )  
                and date(etl_update_date) < DATE_sub(CURRENT_DATE(), interval {date_sub_end_day} day )
                and source_db = '{db_connection_config.source_db}'
                and source_table = '{db_connection_config.source_table}'
                and is_source_delete = 0  
                """
    

    execute_sql( 'target' , delete_sql, db_connection_config, if_return=False)
    df = execute_sql( 'target' , query, db_connection_config, if_return=True)
    if not df.empty:
        df.columns = ['house_keeping_id' ,'ids']
    
    return df


def check_if_synced_id_list_is_expired_over_14_days(db_connection_config, synced_id_list):

    query = f"""select {db_connection_config.timestamp_field_name}
                FROM {db_connection_config.source_db}.{db_connection_config.source_table}
                WHERE {db_connection_config.id_field_name} IN ({synced_id_list})
                and date({db_connection_config.timestamp_field_name}) >= DATE_sub(CURRENT_DATE(), interval 14 day );
             """

    df = execute_sql('source_house_keeping', query, db_connection_config, if_return=True)

    return df.empty


def delete_source_data_by_synced_ids(db_connection_config, synced_id_list, house_keeping_id):
    
    query = f"""DELETE FROM {db_connection_config.source_db}.{db_connection_config.source_table}
                WHERE {db_connection_config.id_field_name} IN ({synced_id_list});
             """
    
    attempt = 0
    max_retries = 10
    success = False
    
    while attempt < max_retries and not success:
        try:
            execute_sql('source_house_keeping', query, db_connection_config, if_return=False)
            
            success = True
            return_msg = f'house_keeping_id_{house_keeping_id}_done'
            return return_msg
        
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}. Retrying in 10 seconds...")
            time.sleep(10)
    
    if not success:
        return f'house_keeping_id_{house_keeping_id}_failed_after_{max_retries}_attempts'
    
    
def update_etl_sync_ids_status(db_connection_config, house_keeping_id):
    
    query = f"""UPDATE {initial_etl_task_connection_config.target_db}.{etl_sync_ids}
                SET is_source_delete = 1
                WHERE house_keeping_id = {house_keeping_id};
             """
    
    attempt = 0
    max_retries = 10
    success = False
    
    while attempt < max_retries and not success:
        try:

            execute_sql('target', query, db_connection_config, if_return=False)
            
            success = True
            return_msg = f'house_keeping_id_{house_keeping_id}_status_updated'
            return return_msg
        
        except Exception as e:
            
            attempt += 1
            print(f"Attempt {attempt} failed: {e}. Retrying in 10 seconds...")
            time.sleep(10)
    
    if not success:
        print(f'house_keeping_id_{house_keeping_id}_status_update_failed_after_{max_retries}_attempts')
        return f'house_keeping_id_{house_keeping_id}_status_update_failed_after_{max_retries}_attempts'

def decompressed_data(ids):
    try:
        decompressed_data = zlib.decompress(ids)
        ids_decode = decompressed_data.decode('utf-8')  
        return ids_decode
    except (zlib.error, UnicodeDecodeError) as e:
        print(f"Decompression or decoding error: {e}")
        return None

def house_keeping_process(db_connection_config):

    try:
        print(f'source_password:{db_connection_config.source_password}')
        ids_to_be_delete_list = get_ids_compress_list_from_etl_sync_ids(db_connection_config)
        
        if not ids_to_be_delete_list.empty:
            df_not_empty_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Start deleting data through records in {etl_sync_ids}'
            send_msg_to_multiple_slack_channel(df_not_empty_msg) 
            
            for index, row in ids_to_be_delete_list.iterrows():
                
                house_keeping_id = row['house_keeping_id']
                ids = row['ids']
                
                # 
                try:
                    synced_id_list = decompressed_data(ids)
                    if not synced_id_list:
                        error_msg = f"Decompression failed for house_keeping_id {house_keeping_id}, skipping."
                        send_msg_to_multiple_slack_channel(error_msg)
                        continue 

                except Exception as decompress_error:
                    error_msg = f"Error decompressing data for house_keeping_id {house_keeping_id}: {str(decompress_error)}"
                    send_msg_to_multiple_slack_channel(error_msg)
                    raise decompress_error  # 
                
                is_expired  = check_if_synced_id_list_is_expired_over_14_days(db_connection_config, synced_id_list)
                print(synced_id_list[:20])
                if is_expired  == True:
                    # 
                    try:
                        # fake_msg = f"this process will excute house_keeping_id:{house_keeping_id}, delete {db_connection_config.source_db}.{db_connection_config.source_table} from id:{synced_id_list[:20]}..."
                        # send_msg_to_multiple_slack_channel(fake_msg)
                        delete_source_data_by_synced_ids(db_connection_config, synced_id_list, house_keeping_id)

                    except Exception as delete_error:
                        error_msg = f"Error deleting source data for house_keeping_id {house_keeping_id}: {str(delete_error)}"
                        send_msg_to_multiple_slack_channel(error_msg)
                        raise delete_error  # 
                    
                     
                    try:
                        success = update_etl_sync_ids_status(db_connection_config, house_keeping_id)
                        if not success:
                            error_msg = f"Failed to update status for house_keeping_id {house_keeping_id}."
                            send_msg_to_multiple_slack_channel(error_msg)

                    except Exception as update_error:
                        error_msg = f"Error updating etl_sync_ids status for house_keeping_id {house_keeping_id}: {str(update_error)}"
                        send_msg_to_multiple_slack_channel(error_msg)
                        raise update_error  # 
                else:
                    do_not_delete_msg = f"house_keeping_id:{house_keeping_id} is not expired, skip house-keeping process {synced_id_list[:20]}..."
                    print(do_not_delete_msg)

                time.sleep(1)

            df_empty_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：House-Keeping completed, no expired data remains to be deleted'
            send_msg_to_multiple_slack_channel(df_empty_msg)

        else:
            skip_task_msg = f'skip {db_connection_config.source_db}.{db_connection_config.source_table} hosue keeping process'
            send_msg_to_multiple_slack_channel(skip_task_msg)       

        return 'No need to delete data anymore'

    except Exception as e:
        error_msg = f"Error in house_keeping_process: {str(e)}"
        send_msg_to_multiple_slack_channel(error_msg)
        raise e  # 


def get_time_setting():
    
    # set time zone as UTC+8
    utc_8 = timezone(timedelta(hours=8))
    current_time = datetime.now(utc_8)

    # get date
    today = current_time.date()

    # switch time_str into datetime format
    start_time = datetime.combine(today, datetime.strptime(optimize_start_time_str, "%H:%M").time()).replace(tzinfo=utc_8)
    end_time = datetime.combine(today, datetime.strptime(optimize_end_time_str, "%H:%M").time()).replace(tzinfo=utc_8)    
    
    return utc_8, current_time, start_time, end_time

def calculate_wait_time_seconds(current_time, start_time):

    wait_time = (start_time - current_time).total_seconds() + 1
    
    return max(0, int(wait_time))

def optimize_starting_stage():
    #
    
    if is_optimization_active == 0:
        optimize_pause_msg = f'============= is_optimization_active == 0, pause optimization tasks today ============='
        send_msg_to_multiple_slack_channel(optimize_pause_msg)       
        
    else:
        optimize_start_msg = f'============= Waiting for {optimize_start_time_str} to run optimization tasks on mysql ============='
        send_msg_to_multiple_slack_channel(optimize_start_msg)   
        
        utc_8, current_time, start_time, end_time = get_time_setting()
        wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)
        
        while current_time < start_time:
            wait_msg = f"Current time is {current_time.time()}. Waiting {wait_time_seconds} seconds until {start_time.time()} to start optimization."
            send_msg_to_multiple_slack_channel(wait_msg)        
            time.sleep(wait_time_seconds)
            
            current_time = datetime.now(utc_8)
            wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)


def optimize_source_table(db_connection_config):

    utc_8, current_time, start_time, end_time = get_time_setting()
    wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)
    
    # wait for optimization
    while current_time < start_time:
        time.sleep(wait_time_seconds)
        
        current_time = datetime.now(utc_8)
        wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)

    # optimize duration 
    if start_time <= current_time <= end_time:
        optimize_start_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Start running the optimization process'
        send_msg_to_multiple_slack_channel(optimize_start_msg)

        try:
            query = f'OPTIMIZE TABLE {db_connection_config.source_db}.{db_connection_config.source_table};'
            result = execute_sql('source_house_keeping', query, db_connection_config, if_return=True)
            print("Database Result:", result)
            
            optimize_end_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Optimization completed'
            send_msg_to_multiple_slack_channel(optimize_end_msg)


        except Exception as e:
            error_msg = f'Error occurred while optimizing {db_connection_config.source_db}.{db_connection_config.source_table}: {e}'
            send_msg_to_multiple_slack_channel(error_msg)

    else:
        # over 12:00 skip
        skip_msg = f"Current time is {current_time.time()}. Outside the allowed range of {start_time.time()} to {end_time.time()} UTC+8. Skipping optimization process."
        send_msg_to_multiple_slack_channel(skip_msg)

    return True

def decide_optimization():
    if is_optimization_active == 1:
        return "optimize_starting_stage" 
    else:
        return "skip_optimization_stage"


airflow_task_id = Variable.get("airflow_task_id")
is_optimization_active = int(Variable.get("is_optimization_active"))
optimize_start_time_str = Variable.get("optimize_start_time_str")
optimize_end_time_str = Variable.get("optimize_end_time_str")
date_sub_start_day = Variable.get("mysql_house_keeping_date_sub_start_day")
date_sub_end_day = Variable.get("mysql_house_keeping_date_sub_end_day")
crypto_key = Variable.get("crypto_key")


etl_task_table = f'etl_task_record_{airflow_task_id}'
etl_sync_ids = f'etl_sync_ids_{airflow_task_id}'
is_poc = Variable.get("is_poc")

# get db config by environment
if is_poc == '1':
    target_db_config = db_config("POC_MySQL_account")  
else:
    target_db_config = db_config("PROD_MySQL_account")

initial_etl_task_connection_config = etl_task_connection_config(None, None, None, None, None, target_db_config.username, target_db_config.password, target_db_config.host, target_db_config.db, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)

tasks_df = etl_tasks(airflow_task_id, etl_task_table, initial_etl_task_connection_config)

# define DAG
with DAG(
    "myssql_house_keeping_tasks",
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description="Perform housekeeping tasks based on the id records in the etl_sync_ids table ",
    schedule_interval='0 22 * * *',  # UTC+0
    catchup=False,
    tags=[ "mysql", "etl", "Housekeeping"],
) as dag:

    # housekeeping start
    housekeeping_starting_stage_op = PythonOperator(
        task_id="housekeeping_starting_stage",
        python_callable=starting_stage,
        trigger_rule=TriggerRule.ALL_DONE 
    )

    # BranchPythonOperator
    decide_optimization_op = BranchPythonOperator(
        task_id='decide_optimization',
        python_callable=decide_optimization,
    )

    # optimization_start
    optimize_starting_stage_op = PythonOperator(
        task_id="optimize_starting_stage",
        python_callable=optimize_starting_stage,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # housekeeping_tasks_group 
    with TaskGroup("housekeeping_tasks_group") as housekeeping_tasks_group:
        previous_task = None
        for index, row in tasks_df.iterrows():
            db_connection_config = create_etl_task_connection_config(row)
            task_id_prefix = f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}"

            current_task = PythonOperator(
                task_id=f"{task_id_prefix}_house_keeping_process",
                python_callable=house_keeping_process,
                op_kwargs={'db_connection_config': db_connection_config},
                trigger_rule=TriggerRule.ALL_DONE
            )
        
            if previous_task:
                previous_task >> current_task

            previous_task = current_task

    # housekeeping finished
    all_housekeeping_done = EmptyOperator(
        task_id="all_housekeeping_done",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # optimize_tasks_group 
    with TaskGroup("optimize_tasks_group") as optimize_tasks_group:
        for index, row in tasks_df.iterrows():
            db_connection_config = create_etl_task_connection_config(row)
            task_id_prefix = f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}"

            PythonOperator(
                task_id=f"{task_id_prefix}_optimize_process",
                python_callable=optimize_source_table,
                op_kwargs={'db_connection_config': db_connection_config},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

    # skip optimization
    skip_optimization_stage = EmptyOperator(
        task_id="skip_optimization_stage"
    )

    # skip optimization
    end_optimization_stage = EmptyOperator(
        task_id="end_optimization_stage"
    )



    # dependency
    housekeeping_starting_stage_op >> housekeeping_tasks_group >> all_housekeeping_done >> decide_optimization_op
    decide_optimization_op >> [optimize_starting_stage_op, skip_optimization_stage] 
    optimize_starting_stage_op >> optimize_tasks_group
    skip_optimization_stage >> end_optimization_stage