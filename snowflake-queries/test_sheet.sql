create database DEMO_DB;
create schema STAGING;
use schema DEMO_DB.STAGING;


create warehouse warehouse_name;

create role ANALYST;
-- create user ANALYST password 'analyst';
grant usage on warehouse_name to role ANALYST read-only;


create role TESTER;
grant role  TESTER to role ANALYST read-only;

-- grant select role of ANALYST set to TESTER;
-- grant usage on warehouse_name to role ANALYST;


create role DEVELOPER;
grant role  TESTER to role DEVELOPER read-only;
-- grant usage on warehouse_name to role ANALYST;

create role PROJECT_ADMIN;
grant role  DEVELOPER to role PROJECT_ADMIN read-only;
grant usage on warehouse_name to role ANALYST;
--warehouse nahi likha tha sir ne notice kiya


---------chatgpt answers-----------------------------------------------------------------
-- Create roles
CREATE ROLE ANALYST;
CREATE ROLE TESTER;
CREATE ROLE DEVELOPER;
CREATE ROLE PROJECT_ADMIN;

-- Create role hierarchy
GRANT ROLE ANALYST TO ROLE TESTER;
GRANT ROLE TESTER TO ROLE DEVELOPER;
GRANT ROLE DEVELOPER TO ROLE PROJECT_ADMIN;

-- Grant privileges to ANALYST
GRANT USAGE ON WAREHOUSE warehouse_name TO ROLE ANALYST;
GRANT USAGE ON DATABASE DEMO_DB TO ROLE ANALYST;
GRANT USAGE ON SCHEMA DEMO_DB.STAGING TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO_DB.STAGING TO ROLE ANALYST;

-- Grant privileges to TESTER
GRANT USAGE ON DATABASE PRACTISE_DB TO ROLE TESTER;
GRANT USAGE ON SCHEMA PRACTISE_DB.TARGET TO ROLE TESTER;
GRANT SELECT ON ALL TABLES IN SCHEMA PRACTISE_DB.TARGET TO ROLE TESTER;

-- Grant privileges to DEVELOPER
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA DEMO_DB.STAGING TO ROLE DEVELOPER;

-- Grant privileges to PROJECT_ADMIN
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA PRACTISE_DB.TARGET TO ROLE PROJECT_ADMIN;
------------------------------------------------------------------------------

-- 	Create role hierarchy to provide access mentioned below
-- ANALYST: Should have access of
-- Usage on warehouse 'warehouse_name'
-- Read only on 'DEMO_DB.STAGING'
-- TESTER: Should have all the access of ANALYST 
-- And read only on 'PRACTISE_DB.TARGET'
-- DEVELOPER: Should have all the access of TESTER
-- And execute DDL/DML operations on 'DEMO_DB.STAGING'
-- PROJECT_ADMIN: Should have all the access of DEVELOPER
-- And execute DDL/DML operations on 'PRACTISE_DB.TARGET'






create or replace table employee(
    EmployeeID int, 
    FirstName varchar(30),
    LastName Varchar(30),
    Dep_Id varchar(30),
    Designation varchar(30),
    HireDate date,
    Salary int,
    Loc_Id int,
    Status varchar(30)
);


create or replace storage integration s3_int
 type = external_stage
 enabled = true
 storage_provider = 's3'
 storage_aws_role_arn = 'arn:aws:iam::992382567849:role/test_role_sam'
 storage_allowed_locations = ('s3://snowflake-int-bucket-satyam');

 desc integration s3_int;

 create or replace file format ff
    type = 'csv',
    field_delimiter = ','

    skip_header = 1;

create or replace stage s3_stage
    storage_integration = s3_int
    url = 's3://snowflake-int-bucket-satyam'
    file_format = ff;

list @s3_stage;

copy into employee 
 from @s3_stage/Employee_data.csv
 file_format = ff;

 select * from employee;


select FirstName, DEP_Id from employee;

select  Dep_Id ,avg(salary) from employee group by(Dep_Id);

 select FirstName , LastName from employee e join (
     select  Dep_Id ,avg(salary) AS avg_salary  from employee group by(Dep_Id)
 ) as dept_avg on e.Dep_Id  = dept_avg.Dep_Id  where e.salary > dept_avg.avg_salary;


 select name , salary from table1 join table2 on table1.id = table2.id where table1.id = table2.id;


--75 average 