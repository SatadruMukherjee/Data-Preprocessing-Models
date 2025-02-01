DROP DATABASE IF EXISTS snowflake_llm_poc;
CREATE Database snowflake_llm_poc;
use snowflake_llm_poc;

--create the external stage with directory table enabled
create or replace stage snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt url="s3://{}/" 
credentials=(aws_key_id=''
aws_secret_key='')
Directory=(ENABLE=TRUE);

ls @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt;

alter stage snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt refresh;

SELECT * FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt);


--read pdf files & text chunking
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


--RAG Storage
create or replace TABLE snowflake_llm_poc.PUBLIC.DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- Relative path to the PDF file
    SIZE NUMBER(38,0), -- Size of the PDF
    Index Number(38,0), --Index no. of the chunk
    CHUNK VARCHAR(16777216), -- Piece of text
    Embedding_Vector VECTOR(FLOAT, 768)
);


--vector embedding
insert into snowflake_llm_poc.PUBLIC.docs_chunks_table (relative_path, size, Index,chunk,Embedding_Vector)
with splitted_data as (SELECT RELATIVE_PATH,SIZE,read_pdf_and_split(BUILD_SCOPED_FILE_URL( @snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt , RELATIVE_PATH )) as pdf_text_split FROM directory(@snowflake_llm_poc.PUBLIC.Snow_stage_directory_table_yt))
select Relative_path,SIZE,f.Index,trim(f.value,'"') as chunk,SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', trim(f.value,'"')) as Embedding_Vector from splitted_data , lateral flatten(pdf_text_split) f ;

select * from snowflake_llm_poc.PUBLIC.docs_chunks_table;

--RAG
SET user_question = 'Safety Precautions for the Mondracer Infant Bike?';
select $user_question;

SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            $user_question
            ), Embedding_Vector
            ) limit 5)
        ),
        ' Question: ', 
        $user_question,
        'Answer: '
    )
) as response;

--Current Chat History
'user : Safety Precautions for the Mondracer Infant Bike?
ai:  Safety precautions for the Mondracer Infant Bike include:
1. Ensuring your infant wears appropriate safety gear, including a properly fitted helmet.
2. Choosing safe riding locations, such as flat, smooth surfaces away from traffic and obstacles.
3. Adjusting the handlebar height to the appropriate level for your infant''s comfort and safety.
4. Checking that all bolts and fasteners are tightened securely.
5. Making proper adjustments to ensure your infant''s comfort and safety while riding.
6. Placing your infant on the bike, ensuring they are wearing their helmet and seated securely on the saddle.
7. Inflating the tires to the recommended pressure as indicated on the sidewall of the tire.
8. Using the special allen wrench provided for the assembly.'


--User's next question
SET user_question = 'What the bike is for?';
select $user_question;

SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            $user_question
            ), Embedding_Vector
            ) limit 5)
        ),
        ' Question: ', 
        $user_question,
        'Answer: '
    )
) as response;

--current question improvement using chat history
select snowflake.cortex.complete('mistral-large', concat(
'Based on the chat history below and the question, generate a query that extend the question
 with the chat history provided. The query should be in natual language. 
Answer with only the query. Do not add any explanation.

<chat_history>',

'user : Safety Precautions for the Mondracer Infant Bike?
ai:  Safety precautions for the Mondracer Infant Bike include:
1. Ensuring your infant wears appropriate safety gear, including a properly fitted helmet.
2. Choosing safe riding locations, such as flat, smooth surfaces away from traffic and obstacles.
3. Adjusting the handlebar height to the appropriate level for your infant''s comfort and safety.
4. Checking that all bolts and fasteners are tightened securely.
5. Making proper adjustments to ensure your infant''s comfort and safety while riding.
6. Placing your infant on the bike, ensuring they are wearing their helmet and seated securely on the saddle.
7. Inflating the tires to the recommended pressure as indicated on the sidewall of the tire.
8. Using the special allen wrench provided for the assembly.',

'</chat_history>

<question>',

$user_question,

'</question>'));



SET user_question = 'Could you provide more details about the purpose and intended use of the Mondracer Infant Bike, considering the safety precautions mentioned earlier?';
select $user_question;

SELECT snowflake.cortex.complete(
    'mistral-large', 
    CONCAT( 
        'Answer the question based on the context. Be concise.','Context: ',
        (
            select listagg(chunk,', ') from (SELECT CHUNK from snowflake_llm_poc.PUBLIC.docs_chunks_table
            ORDER BY VECTOR_L2_DISTANCE(
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', 
            $user_question
            ), Embedding_Vector
            ) limit 5)
        ),
        ' Question: ', 
        $user_question,
        'Answer: '
    )
) as response;
