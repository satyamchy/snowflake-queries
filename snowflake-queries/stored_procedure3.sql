
create database samdb;
use database samdb;
create or replace schema sam_schema;
use schema sam_schema;

create or replace user satya password ='Satyam@9693342101' 
comment = 'a fully managed user by satyam shekhar itself';

create warehouse sam_vm;
grant usage on warehouse sam_vm to role sam_role;

show roles;
create or replace role sam_role;
grant role accountadmin to role sam_role;
grant role sam_role to user satya;

show roles like 'sam_role';
show users like 'satya';

grant usage on database samdb to role sam_role;
grant usage on database samdb to user satya;
show databases;



----------------------------=================================================
create table orders(
order_id int,
orders_date date,
amount int
);
insert into orders values(1, '2025-01-10', 1000), (2, '2004-02-07', 2000), (3, '2005-01-10', 2000);
select * from orders;


create or replace procedure delete_order(id int, orders string)
returns number
language sql
as
declare 
    cnt number default (select count(*) from orders);
    begin 
     delete from identifier(:orders) where order_id = :id;
     cnt := cnt - (select count(*) from orders);
     return cnt;
    end;

call delete_order(3, 'orders');
---------------------------------------------==========================================================


create or replace procedure pro(namee1 string)
returns string
language sql
as
declare c int;
begin
select count(*) into c from t3 where namee = :namee1;
    if( c=0 )then
    insert into t3(id, namee, loc, age) values(5, namee1, meow, 34);
    return 'insertion succesful';
    else return 'customer already exists';
    end if;
end;
-----------------------------------------------------------=========================================================
--✅  Logging Loop Values to a Temporary Table

-- Step 1: Create a temporary table to store log messages
create or replace temporary table loop_log (log_message string);
select * from loop_log;
 --call system$print('Current i: ' || i); 
 --Snowflake SQL stored procedures, you cannot use call system$print(...) directly inside a BEGIN...END block

declare 
    msg string default 'aaaa';
begin
    let cnt number default 10000;
    let i number default 0;
        while( i <= 10000) loop
            insert into loop_log values ('Current i: ' || :i);
            msg := msg || i || ' - ';  
            i := i+1000;
        end loop;
        return msg;
end;
-----------------------------------------------------------=========================================================
-- ✅  Stored Procedure to Return Employee Names by Department

create or replace  table employees (first_name string, employee_dept string );
INSERT INTO employees (first_name, employee_dept, dept_id) VALUES
('Alice',   'HR',           101),
('Bob',     'Engineering',  102),
('Charlie', 'Marketing',    103),
('David',   'Engineering',  102),
('Eva',     'Finance',      104),
('Farah',   'HR',           101),
('George',  'Engineering',  102),
('Hannah',  'Marketing',    103),
('Ivan',    'Finance',      104),
('Julia',   'HR',           101);
select * from employees;


create or replace procedure get_employee_names_by_dept_id(dept_id int, employees string)
returns table (first_name string)
language sql
as
begin
    return table (
         select first_name from identifier(:employees) where dept_id = :dept_id
    );
end;
-- identifier(:employees) is used for dynamic SQL, but RETURN TABLE(...) expects a static SQL query.
        -- Snowflake does not allow dynamic identifiers (like table names) inside RETURN TABLE.

call get_employee_names_by_dept_id(101, 'employees');
---------------------------------------------------------------------

create or replace procedure get_employee_names_by_dept(dept_id int, employees string)
returns table (first_name string)
language sql
as
declare
    sql_command string;
begin
     -- Build dynamic SQL
    sql_command := 'SELECT first_name FROM ' || :employees || ' WHERE dept_id = ' || :dept_id;

    -- Execute the SQL
    EXECUTE IMMEDIATE :sql_command;

    -- Return the result of the last query
    RETURN TABLE (RESULT_SCAN(LAST_QUERY_ID()));
end;

call get_employee_names_by_dept(101, 'employees');

----------------------------------------------------------

       CREATE OR REPLACE SEQUENCE my_sequence 
  START WITH 1 
  INCREMENT BY 1 
  MINVALUE 1 
  MAXVALUE 999999 
  CYCLE (optional); 

SELECT my_sequence.NEXTVAL; 
  SELECT my_sequence.NEXTVAL; 
