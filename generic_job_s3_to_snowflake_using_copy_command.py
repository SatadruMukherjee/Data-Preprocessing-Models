"""
pip install pandas
pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.3.10/tested_requirements/requirements_38.reqs -t .
pip install snowflake-connector-python==2.3.10 -t .
"""
import pandas as pd
import snowflake.connector as sf

user=""
password="@"
account=""
conn=sf.connect(user=user,password=password,account=account)


def run_query(query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()


def run_query_single_value_return(query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    records = cursor.fetchone()[0]
    cursor.close()
    return records


def run_copy_command(query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    query_id=cursor.sfqid
    cursor.close()
    return query_id


def execute_copy_cmd():
    print("Reading the metadata file...")

    df=pd.read_csv('{}')

    for index,row in df.iterrows():
        database=row['DATABASE']
        schema=row['SCHEMA']
        table=row['TABLE_NAME']
        external_stage_object=row['STAGE_OBJECT']
        s3_file_path=row['S3_FILE_PATH_TO_BE_APPENDED_WITH_STAGE_OBJECT']
        warehouse=row['WAREHOUSE']
        snowflake_role=row['SNOWFLAKE_ROLE']
        file_format=row['FILE_FORMAT']
        pattern=row['PATTERN']


        #set up the env of execution
        statement_1 = 'use warehouse ' + warehouse
        statement2 = 'alter warehouse ' + warehouse + " resume  IF SUSPENDED"
        statement3 = "use database " + database
        statement4 = "use role " + snowflake_role
        statement5=  "use schema " + schema
        run_query(statement_1)
        run_query(statement2)
        run_query(statement3)
        run_query(statement4)
        run_query(statement5)

        #executing the copy command
        copy_command="""copy into {}.{}.{} from @{}/{}/ FILE_FORMAT={} PATTERN='{}' ON_ERROR=CONTINUE""".format(database,schema,table,external_stage_object,s3_file_path,file_format,pattern)
        query_id_of_the_copy_command=run_copy_command(copy_command)

        #check whether copy command picked up any file or not
        detecting_copy_command_picked_up_file_or_not="""SELECT "status"    FROM  TABLE(RESULT_SCAN('{}'))  limit 1;""".format(query_id_of_the_copy_command)
        first_value_of_status_in_copy_command_output= run_query_single_value_return(detecting_copy_command_picked_up_file_or_not)
        print("First value of result-set of the above copy command execution : {}".format(first_value_of_status_in_copy_command_output))
        count_no_of_rows_inserted_due_to_copy_command=0

        if(first_value_of_status_in_copy_command_output!='Copy executed with 0 files processed.'):
            #rows inserted by copy command
            command_to_get_no_of_rows_inserted_due_to_copy_command="""SELECT sum("rows_loaded") FROM  TABLE(RESULT_SCAN('{}'));""".format(query_id_of_the_copy_command)
            count_no_of_rows_inserted_due_to_copy_command=run_query_single_value_return(command_to_get_no_of_rows_inserted_due_to_copy_command)
        print("No. of rows inserted due to this copy command execution : {}".format(count_no_of_rows_inserted_due_to_copy_command))

        #Capture the rejected records
        rejects_collector = """insert into {}.{}.copy_cmd_rejects select '{}' QUERY_ID,'{}' TABLE_NAME, CURRENT_TIMESTAMP(),A.* from table(validate({}.{}.{},job_id=>'{}')) A""".format(database,schema,query_id_of_the_copy_command,table,database,schema,table,query_id_of_the_copy_command)
        run_query(rejects_collector)


        #get total number of rejected records
        rejected_records="select count(distinct ROW_NUMBER) from {}.{}.copy_cmd_rejects where QUERY_ID='{}'".format(database,schema,query_id_of_the_copy_command)
        count_of_rejected_records=run_query_single_value_return(rejected_records)

        #audit the records
        audit_copy="""insert into {}.{}.COPY_AUDIT select QUERY_ID,QUERY_TEXT,DATABASE_NAME,'{}' ROWS_INSERTED,'{}' 
                    ROWS_REJECTED,SCHEMA_NAME,ROLE_NAME,WAREHOUSE_NAME,WAREHOUSE_SIZE,EXECUTION_STATUS,ERROR_MESSAGE,EXECUTION_TIME ,current_timestamp() ETL_TS 
                    FROM table(information_schema.query_history()) where query_type='COPY' AND QUERY_ID='{}' """.format(database,schema,count_no_of_rows_inserted_due_to_copy_command,
                                                                                                                        count_of_rejected_records,query_id_of_the_copy_command)
        run_query(audit_copy)


execute_copy_cmd()