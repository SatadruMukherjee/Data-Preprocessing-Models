create or replace table source_table( emp_no int,emp_name text,salary int, hra int );
  
    

INSERT INTO source_table VALUES (100, 'A' ,2000, 100),
(101, 'B' ,5000, 300),
(102, 'C' ,6000, 400),
(103, 'D' ,500, 50),
(104, 'E' ,15000, 3000),
(105, 'F' ,150000, 20050);


create or replace sequence seq_01 start = 1 increment = 1;


create or replace table target_table( surrogate_key int default seq_01.nextval,emp_no int,emp_name text,salary int, 
                                     hra int,start_date string default current_timestamp()::string ,end_date string,activeflag text default 'Y' );
   
SELECT * FROM source_table;

select * from target_table;  


select * from updated_emp_id;





INSERT INTO source_table VALUES (110, 'AB' ,5600, 180);
INSERT INTO source_table VALUES (115, 'CD' ,5670, 185);


update source_table set salary=5690 where emp_name='A';
update source_table set HRA=645 where emp_name='CD';

delete from source_table where emp_name='B';


INSERT INTO source_table VALUES (1010, 'B' ,5600, 180);
update source_table set salary=7000 where emp_name='A';
delete from source_table where emp_name='C';

select * from    target_table;  


