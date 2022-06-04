drop database if exists ramu;

create database ramu;

create or replace table ramu.public.employees(employee_id number,
                     empl_join_date date,
                     dept varchar(10),
                     salary number,
                     manager_id number);
                     
insert into ramu.public.employees values(1,'2014-10-01','HR',40000,4),
                                 (2,'2014-09-01','Tech',50000,9),
                                 (3,'2018-09-01','Marketing',30000,5),
                                 (4,'2017-09-01','HR',10000,5),
                                 (5,'2019-09-01','HR',35000,9),
                                 (6,'2015-09-01','Tech',90000,4),
                                 (7,'2016-09-01','Marketing',20000,1);
                                 
select * from ramu.public.employees;

CREATE OR REPLACE TABLE access_management_lookup (role string, dept_name string);

INSERT INTO access_management_lookup VALUES ('HR_ADMIN', 'HR'), ('TECH_ADMIN', 'Tech'),('MARKETING_ADMIN', 'Marketing');

select * from access_management_lookup;



create or replace row access policy dept_level_access as (dept varchar) returns boolean ->
   current_role()='ACCOUNTADMIN'
      or exists (
            select 1 from access_management_lookup
              where role = current_role()
                and dept_name = depta
          );


ALTER TABLE ramu.public.employees ADD ROW ACCESS POLICY dept_level_access ON (dept);

select * from ramu.public.employees;

               


create or replace role HR_Admin;
create or replace role Tech_Admin;
create or replace role Marketing_Admin;

grant usage on warehouse compute_Wh to role HR_Admin;
grant usage on warehouse compute_Wh to role Tech_Admin;
grant usage on warehouse compute_Wh to role Marketing_Admin;

grant usage on database ramu to role HR_Admin;
grant usage on database ramu to role Tech_Admin;
grant usage on database ramu to role Marketing_Admin;

grant usage on schema public to role HR_Admin;
grant usage on schema public to role Tech_Admin;
grant usage on schema public to role Marketing_Admin;

grant select on table ramu.public.employees to role HR_Admin;
grant select on table ramu.public.employees to role Tech_Admin;
grant select on table ramu.public.employees to role Marketing_Admin;


create or replace user jadu_hr password = '123456';
grant role HR_Admin to user jadu_hr;



create or replace user mimo_marketing password = '456789';
grant role Marketing_Admin to user mimo_marketing;


create or replace user jimo_tech password = '147258';
grant role Tech_Admin to user jimo_tech;


drop user jimo_tech;
drop user mimo_marketing;
drop user jadu_hr;

drop role HR_Admin;
drop role Tech_Admin;
drop role Marketing_Admin;
