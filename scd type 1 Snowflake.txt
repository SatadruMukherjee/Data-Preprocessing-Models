create or replace database ramu;
use ramu;

create or replace sequence seq_01 start = 1 increment = 1;

create or replace table source_table( emp_no int,emp_name text,salary int, hra int );
  
    

INSERT INTO source_table VALUES (100, 'A' ,2000, 100),
(101, 'B' ,5000, 300),
(102, 'C' ,6000, 400),
(103, 'D' ,500, 50),
(104, 'E' ,15000, 3000),
(105, 'F' ,150000, 20050);

select * from source_table; 


create or replace table target_table(surrogate_key int default seq_01.nextval,emp_no int,emp_name text,salary int, 
                                     hra int);
                                     
                                     
select * from target_table;  


INSERT INTO PUBLIC.target_table(emp_no, emp_name, salary, hra) 
SELECT  t.emp_no, t.emp_name, t.salary, t.hra FROM PUBLIC.source_table t 
LEFT JOIN PUBLIC.target_table d ON  d.emp_no = t.emp_no WHERE ( d.emp_no IS NULL);


select * from target_table;  



update source_table set salary=5690 where emp_name='A';

select * from source_table; 
select * from target_table;  


UPDATE PUBLIC.target_table d SET  emp_name = t.emp_name,  salary = t.salary,  hra = t.hra 
FROM PUBLIC.source_table t WHERE  d.emp_no = t.emp_no AND (( d.emp_name <> t.emp_name) OR ( d.salary <> t.salary) OR ( d.hra <> t.hra));


select * from target_table;  

update source_table set salary=6000 where emp_name='B';
update source_table set HRA=3000 where emp_name='B';
INSERT INTO source_table VALUES (1001, 'MG' ,2000, 100);

select * from source_table; 
select * from target_table;  


INSERT INTO PUBLIC.target_table(emp_no, emp_name, salary, hra) 
SELECT  t.emp_no, t.emp_name, t.salary, t.hra FROM PUBLIC.source_table t 
LEFT JOIN PUBLIC.target_table d ON  d.emp_no = t.emp_no WHERE ( d.emp_no IS NULL);
UPDATE PUBLIC.target_table d SET  emp_name = t.emp_name,  salary = t.salary,  hra = t.hra 
FROM PUBLIC.source_table t WHERE  d.emp_no = t.emp_no AND (( d.emp_name <> t.emp_name) OR ( d.salary <> t.salary) OR ( d.hra <> t.hra));

select * from source_table; 
select * from target_table;  