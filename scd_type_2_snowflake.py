"""

Required Installation :
-------------------------
python --version
python -m pip install --upgrade pip
pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.7.3/tested_requirements/requirements_38.reqs -t .
pip install snowflake-connector-python==2.7.3 -t .
pip install pandas -t .
pip install "snowflake-connector-python[pandas]" -t .

"""
import snowflake.connector as sf
import pandas as pd
from config import *


conn=sf.connect( account=account,
          user=user,
          password=password,
          warehouse=warehouse,
          role=role,
          database=database,
          schema=schema)



def run_query(conn, query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()


def run_query_one_values(conn, query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    records=cursor.fetchone()[0]
    cursor.close()
    return records




def run_query_pandas_df(conn, query):
    print("Executing the query : {}".format(query))
    cursor = conn.cursor()
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    cursor.close()
    return df

def list_of_columns_in_order(conn,table_name):
    """

    :param conn:
    :param table_name: complete table name for which column names have to be extracted
    :return: columns of the table in-order
    """
    cursor = conn.cursor()
    splitted_data=table_name.split(".")
    query="""select listagg(COLUMN_NAME,',') within group(order by ORDINAL_POSITION ) from 
"{}"."INFORMATION_SCHEMA"."COLUMNS" WHERE  TABLE_SCHEMA='{}' and upper(TABLE_NAME)=upper('{}') ;""".format(splitted_data[0],splitted_data[1],splitted_data[2])
    print("Executing the query : {}".format(query))
    cursor.execute(query)
    records = cursor.fetchone()[0]
    print("List of columns : {}".format(records))
    cursor.close()
    return records


def insert_handler(source_table,target_table,primary_key_column_name,column_list):
    """

    :param source_table: name of the source table
    :param target_table: name of the target table
    :param primary_key_column_name: primary key of the source
    :return: null
    """
    print("**************************************** Handling Insert ****************************************")
    insert_query="""insert into {} ({}) select {} from {}
                    where {} not in (select distinct {} from {});""".format(target_table,column_list,column_list,
                                                                            source_table,primary_key_column_name,
                                                                            primary_key_column_name,target_table)
    run_query(conn, insert_query)
    print("**************************************** Insert Part done ****************************************")

def update_handler(source_table,target_table,view_name,primary_key_column_name,column_list,column_list_without_primary_key):

    print("**************************************** Handling Update ****************************************")

    view_storing_updated_keys="""create or replace table {} as
    with tgu as (select {}, hash({}) as hash_value_target from {} where {} in (select distinct {} from {}) and  activeflag='Y'),sgu as (select {}, hash({}) as hash_value_source from {} where
    {} in (select distinct {} from {})) select sgu.{} from tgu inner join sgu on tgu.{} = sgu.{} where tgu.hash_value_target != sgu.hash_value_source;""".format\
        (view_name,primary_key_column_name,column_list_without_primary_key,target_table,primary_key_column_name,primary_key_column_name,
                                                              source_table ,primary_key_column_name,column_list_without_primary_key,source_table,primary_key_column_name,primary_key_column_name,
                                                              target_table,primary_key_column_name,primary_key_column_name,primary_key_column_name)
    run_query(conn, view_storing_updated_keys)
    update_end_timestamp="""UPDATE {} SET end_date =current_timestamp()::string  where {}  in (select * from updated_emp_id) and activeflag='Y';""".format(target_table,primary_key_column_name)
    run_query(conn, update_end_timestamp)

    update_active_flag="""UPDATE {} set activeflag = 'N' where {} in (select * from updated_emp_id) and activeflag='Y';""".format(target_table,primary_key_column_name)
    run_query(conn, update_active_flag)

    insert_query="""
    insert into {}({}) select {} from {} where {} in (select * from updated_emp_id);""".format(target_table,column_list,column_list,source_table,primary_key_column_name)
    run_query(conn, insert_query)

    print("**************************************** Update Part done ****************************************")


def delete_handler(source_table,target_table,primary_key_column_name):


    print("**************************************** Handling Delete ****************************************")


    query1="""UPDATE {} SET end_date =current_timestamp()::string  where {}  in 
(select {} from {} where {} not in (select distinct {}  from {}) and end_date is null) and end_date is null;""".format(target_table,primary_key_column_name,
                                                                                           primary_key_column_name, target_table,primary_key_column_name,
                                                                                             primary_key_column_name,source_table);
    run_query(conn, query1)
    query2="""UPDATE {} SET activeflag='N'  where {}  in 
(select {} from {} where {} not in (select distinct {}  from {}) and activeflag='Y') and activeflag='Y';""".format(target_table,primary_key_column_name,
                                                                                                                            primary_key_column_name,target_table, primary_key_column_name,
                                                                                                          primary_key_column_name,source_table );
    run_query(conn, query2)

    print("**************************************** Delete Part done ****************************************")


def main():
    source_table_name="RAMU.PUBLIC.SOURCE_TABLE"
    target_table_name="RAMU.PUBLIC.TARGET_TABLE"
    primary_key_column_name="EMP_NO"
    columns_in_order=list_of_columns_in_order(conn,source_table_name)
    insert_handler(source_table_name, target_table_name, primary_key_column_name, columns_in_order)

    column_list_without_primary_key_list=[]
    column_list_splitted=columns_in_order.split(",")

    for i in column_list_splitted:
        if(i!=primary_key_column_name):
            column_list_without_primary_key_list.append(i)

    column_list_without_primary_key=','.join(column_list_without_primary_key_list)

    print("Column list apart from Primary Key in Source Table: {}".format(column_list_without_primary_key))
    update_handler(source_table_name, target_table_name, "updated_emp_id", primary_key_column_name, columns_in_order,
                  column_list_without_primary_key)
    delete_handler(source_table_name, target_table_name, primary_key_column_name)



main()