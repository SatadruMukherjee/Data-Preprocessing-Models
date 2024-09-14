--recall Directory Table (PDF Page Count)

DROP DATABASE IF EXISTS snowflake_llm_poc;
CREATE Database snowflake_llm_poc;
use snowflake_llm_poc;

CREATE OR REPLACE function count_no_of_pages_udf(file_name string)
  RETURNS integer
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python','PyPDF2')
  HANDLER = 'main_fn'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2
def main_fn(file_name):
    f=SnowflakeFile.open(file_name,'rb')
    pdf_object=PyPDF2.PdfReader(f);
    return len(pdf_object.pages)
$$;

--create the external stage with directory table enabled & automatic refresh
create or replace stage snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt url="s3://snwoflakeragtest/pdf_knowledge_base/" 
credentials=(aws_key_id=''
aws_secret_key='')
Directory=(ENABLE=TRUE);

alter stage snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt refresh;

SELECT * FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt);

SELECT RELATIVE_PATH,count_no_of_pages_udf(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_page_count FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt);


----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------


--read PDF Content

CREATE OR REPLACE function read_pdf(file_name string)
  RETURNS string
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python','PyPDF2')
  HANDLER = 'main_fn'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2
def main_fn(file_name):
    f = SnowflakeFile.open(file_name, 'rb')
    pdf_object = PyPDF2.PdfReader(f)
    
    # Initialize a variable to hold all the text
    all_text = ""
    
    # Iterate over all the pages and concatenate the text
    for page in pdf_object.pages:
        all_text += page.extract_text().replace('\n',' ')
    
    return all_text
$$;

SELECT RELATIVE_PATH,read_pdf(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt);

----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------

--read PDF Content, get the text data, split into chunks

CREATE OR REPLACE function read_pdf_and_split(file_name string)
  RETURNS ARRAY
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python','PyPDF2','langchain')
  HANDLER = 'main_fn'
AS
$$
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2
import re
from langchain.text_splitter import CharacterTextSplitter

def main_fn(file_name):
    f = SnowflakeFile.open(file_name, 'rb')
    pdf_object = PyPDF2.PdfReader(f)
    
    # Initialize a variable to hold all the text
    all_text = ""
    
    # Iterate over all the pages and concatenate the text
    for page in pdf_object.pages:
        all_text += page.extract_text().replace('\n',' ')
    
    # split documents into chunks
    text_splitter = CharacterTextSplitter(
    separator='.',
    chunk_size=200,
    chunk_overlap=25,
    
    )
    split_docs = text_splitter.split_text(all_text)
    return split_docs
$$;

--get the data
SELECT RELATIVE_PATH,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt);

--flatten the data
with splitted_data as (SELECT RELATIVE_PATH,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select * from splitted_data , lateral flatten(pdf_text_split) f ;

--take specific columns from Flatten Data
with splitted_data as (SELECT RELATIVE_PATH,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select Relative_path,f.Index,f.value as chunk from splitted_data , lateral flatten(pdf_text_split) f ;

--trim the " from chunks
with splitted_data as (SELECT RELATIVE_PATH,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select Relative_path,f.Index,trim(f.value,'"') as chunk from splitted_data , lateral flatten(pdf_text_split) f ;

--Vector Embedding
with splitted_data as (SELECT RELATIVE_PATH,SIZE,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select Relative_path,SIZE,f.Index,trim(f.value,'"') as chunk,SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', trim(f.value,'"')) as Embedding_Vector from splitted_data , lateral flatten(pdf_text_split) f ;

--Storage
create or replace TABLE snowflake_llm_poc.PUBLIC.DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    Index Number(38,0), --Index no. of the chunk
    CHUNK VARCHAR(16777216), -- Piece of text
    Embedding_Vector VECTOR(FLOAT, 768)
);

insert into snowflake_llm_poc.PUBLIC.docs_chunks_table (relative_path, size, Index,chunk,Embedding_Vector)
with splitted_data as (SELECT RELATIVE_PATH,SIZE,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select Relative_path,SIZE,f.Index,trim(f.value,'"') as chunk,SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', trim(f.value,'"')) as Embedding_Vector from splitted_data , lateral flatten(pdf_text_split) f ;

select * from snowflake_llm_poc.PUBLIC.docs_chunks_table;

--Similarity Search
SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'How many people received Ayushman Card in UP?'
            ), Embedding_Vector
            ) limit 1
        ;

--Generation

--Context share
SELECT 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'How many people received Ayushman Card in UP?'
            ), Embedding_Vector
            ) limit 1
        ),
        ' Question: ', 
        'How many people received Ayushman Card in UP?',
        'Answer: '
    );


SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'How many people received Ayushman Card in UP?'
            ), Embedding_Vector
            ) limit 1
        ),
        ' Question: ', 
        'How many people received Ayushman Card in UP?',
        'Answer: '
    )
) as response;

--multiple chunks pass

(SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'How many people received Ayushman Card in UP?'
            ), Embedding_Vector
            ) limit 2);
            
SELECT 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'How many people received Ayushman Card in UP?'
            ), Embedding_Vector
            ) limit 2)
        ),
        ' Question: ', 
        'How many people received Ayushman Card in UP?',
        'Answer: '
    );



SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'Tell about the Film City Project in UP pointwise'
            ), Embedding_Vector
            ) limit 3)
        ),
        ' Question: ', 
        'Tell about the Film City Project in UP pointwise',
        'Answer: '
    )
) as response;

SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            'Give 5 points how UP Govt. is planning for new job opportunities'
            ), Embedding_Vector
            ) limit 5)
        ),
        ' Question: ', 
        'Give 5 points how UP Govt. is planning for new job opportunities',
        'Answer: '
    )
) as response;
