-- // SEt the roles , Warehouses and Databases
USE role ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

USE database MYDB;
--create schema myschema;
use schema MYDB.myschema;

-- ==================================================================================
-- // there are 4 types of tables PERMANENT, temporary , transient, external

-- // creating a permanent table
create or replace table permanent_table(
    id int,
    name string
);

alter table permanent_table set DATA_RETENTION_TIME_IN_DAYS = 80;
desc table permanent_table;
SHOW TABLES LIKE 'PERMANENT_TABLE';

-----------------------------------------------
-- // CREATE A transient table
create or replace TRANSIENT table transient_table(
    id int,
    name string
);
alter table transient_table set DATA_RETENTION_TIME_IN_DAYS = 3; // its maximum retention period is only 1  day

------------------------------------------------------------------
--// CREATE A temporary table
create or replace TEMPORARY table temporary_table(
    id int,
    name string
);
alter table temporary_table set DATA_RETENTION_TIME_IN_DAYS = 1;

show tables;







-- =========================================================================================
-- ========================================================================================

-- // VIEW TYPES  --- view, secure view, materialized view  ===============================
-- // create an Employee table
create or replace table employees(
    id integer,
    name varchar(50),
    department varchar(50),
    salary integer
);
desc table employees;

--// INserting data into the table
Insert into employees (id, name, department, salary) values (1, 'saket kumar', 'HR', 5000),
        (2, 'shamb', 'ceo', 6000),
        (3, 'steve', 'sales', 44000),
        (4, 'dorma', 'IT', 70000 ),
        (5, 'shekhar', 'IT', 90000);
-- // select data from the table        
select * from employees;

-----------------------------
-- // Let's create a view called "it_employees" that only includes the employees from the IT department;

-- create or replace view it_employees as
-- select id, name, salary from employees where department='IT';

-- // select data from the it_employees view
-- select * from it_employees;

-- ---------------------
-- // Let's create a view called "it_employees" that only includes the employees from the HR department;

create or replace secure view hr_employees as
select id, name, salary from employees where department='HR';

--// select data from the it_employees view
select * from hr_employees;

--------------------------------------------------
--// create a view that aggregates the salaries by department
create or replace view employees_salaries as select department, sum(salary) as total_salary from employees group by department;

--// select data from the employees_salaries view
select * from employees_salaries;

------------------------------------------
--// create a view that aggregates the salaries by department
create or replace view employees_salaries as select department, sum(salary) as total_salary from employees group by department;

--// select data from the employees_salaries view
select * from employees_salaries;

------------------------------------------
--// create a materialized view that aggregates the salaries by department
create or replace materialized view materialized_employee_salaries as select department, sum(salary) as total_salary from employees group by department;

--// select data from the materialized_employee_salaries view
select * from materialized_employee_salaries;

show views;
drop table MYDB.MYSCHEMA.employees;
----------------------------------------
alter session set use_cached_result = false;

--=================================================================================================================================













