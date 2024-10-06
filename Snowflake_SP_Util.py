
--Create Database
Create or replace database snowpark_helper;
use snowpark_helper;

-- Create a sample table
CREATE OR REPLACE TABLE sample_table (
    id INTEGER,
    name STRING,
    score INTEGER
);

-- Insert sample data into the table
INSERT INTO sample_table (id, name, score)
VALUES 
(1, 'ALICE', 85),
(2, 'BOB', 90),
(3, 'CHARLIE', 75),
(4, 'DAVID', 60),
(5, 'EVE', 95);

select * from sample_table;


CREATE OR REPLACE PROCEDURE process_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
Handler='run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import * 

def convert_string_to_lowercase(df):
    return df.with_column("name", lower(df["name"]))

def filter_based_on_score(df, threshold):
    return df.filter(df["score"] > threshold)

def write_data_to_table(df, target_table):
    df.write.save_as_table(target_table, mode="overwrite")

def run(session: Session) -> str:
    # Load the data from the source table
    df = session.table("sample_table")
    
    # Apply the transformations
    df_lowercase = convert_string_to_lowercase(df)
    df_filtered = filter_based_on_score(df_lowercase, 80)
    
    # Write the transformed data to the target table
    target_table = "transformed_table"
    write_data_to_table(df_filtered, target_table)
    
    return "Data transformation and write successful!"

$$;

call process_data();

select * from transformed_table;


--concept of imports
Create or replace  stage snowpark_helper.PUBLIC.snowpark_reusable_code url="s3://{}/" 
credentials=(aws_key_id=''
aws_secret_key='');

list @snowpark_helper.PUBLIC.snowpark_reusable_code;



CREATE OR REPLACE PROCEDURE process_data_with_util()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@snowpark_helper.PUBLIC.snowpark_reusable_code/snowpark_modules.zip')
Handler='run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import * 
from snowpark_modules.transformation import *
from snowpark_modules.write_data import *

def run(session: Session) -> str:
    # Load the data from the source table
    df = session.table("sample_table")
    
    # Apply the transformations
    df_lowercase = convert_string_to_lowercase(df)
    df_filtered = filter_based_on_score(df_lowercase, 80)
    
    # Write the transformed data to the target table
    target_table = "transformed_table_with_util"
    write_data_to_table(df_filtered, target_table)
    
    return "Data transformation and write successful!"

$$;

call process_data_with_util();

select * from SNOWPARK_HELPER.PUBLIC.TRANSFORMED_TABLE_WITH_UTIL;





CREATE OR REPLACE PROCEDURE process_data_with_util_and_param(threshold_value Integer)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@snowpark_helper.PUBLIC.snowpark_reusable_code/tf.zip')
Handler='run'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import * 
from tf.transformation import *
from tf.write_data import *

def run(session: Session,threshold_value) -> str:
    # Load the data from the source table
    df = session.table("sample_table")
    
    # Apply the transformations
    df_lowercase = convert_string_to_lowercase(df)
    df_filtered = filter_based_on_score(df_lowercase, threshold_value)
    
    # Write the transformed data to the target table
    target_table = "transformed_table_with_util_and_param"
    write_data_to_table(df_filtered, target_table)
    
    return "Data transformation and write successful!"

$$;

call process_data_with_util_and_param(70);

select * from SNOWPARK_HELPER.PUBLIC.TRANSFORMED_TABLE_WITH_UTIL_AND_PARAM;