#code explanation: https://youtu.be/Rp0RHsT0jIA?si=zFrYgPChEJn4aWv4
#pip install snowflake-connector-python
#pip install "snowflake-connector-python[pandas]" -t .
#pip install pandas -t .


from snowflake.connector import connect
import pandas as pd
import os


def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()

def run_query1(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    records=cursor.fetchone()[0]
    cursor.close()
    return records

def execute_test(
        db_conn,
        script_1,
        script_2,
        comp_operator):
    print("1st SQL Query : ",script_1)
    result_1=run_query1(db_conn,script_1)
    print("2nd SQL Query : ", script_2)
    result_2 = run_query1(db_conn, script_2)
    print("result 1 = " + str(result_1))
    print("result 2 = " + str(result_2))
    # compare values based on the comp_operator
    if comp_operator == "equals":
        return result_1 == result_2
    elif comp_operator == "greater_equals":
        return result_1 >= result_2
    elif comp_operator == "greater":
        return result_1 > result_2
    elif comp_operator == "less_equals":
        return result_1 <= result_2
    elif comp_operator == "less":
        return result_1 < result_2
    elif comp_operator == "not_equal":
        return result_1 != result_2
    # if we made it here, something went wrong
    return False



user=''
password=''
account=""
database=""
warehouse=""
schema=""
role=""
conn = connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role
    )



sql_query ="""select * from dq_check where table_name='dummy_table'"""
cursor = conn.cursor()
cursor.execute(sql_query)

df=cursor.fetch_pandas_all()
cursor.close()

test_case_output_df=pd.DataFrame(columns=['Check_Description','Status'])

for index,row in df.iterrows():
    table_name=row["TABLE_NAME"]
    description=row['DESCRIPTION']
    print('*'*100)
    print("Performing check : ",description)
    sql_query_1=row['SQL_QUERY_1']
    sql_query_2=row['SQL_QUERY_2']
    comparison_type=row['COMPARISON_TYPE']
    outcome=execute_test(
        conn,
        sql_query_1,
        sql_query_2,
        comparison_type)
    testcase_pass_fail= "Pass" if  outcome else "Failed"
    print("Testcase Results : ",testcase_pass_fail)
    new_row=({'Check_Description': description, 'Status': testcase_pass_fail})
    test_case_output_df = pd.concat([test_case_output_df, pd.DataFrame([new_row])], ignore_index=True)
    print('*' * 100)

print(test_case_output_df)