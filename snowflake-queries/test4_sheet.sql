use schema samdb.sam_schema;

-- 1. Undrop a table using time travel
-- 2. Reload a truncated table using time travel
-- 3. Create a sequence with incremental of 1 and use it in a table for primary key
-- 4. Create 2 procedures to insert last month data from one table to another table 
-- 	(If it is new record use "insert" and if it is old record, use "Delete and Insert")
-- 	(If it is new record use "insert" and if it is old record, use "Update")
-- 5. Create 2 tasks to run the above procedures on 1st of every month
-- 6. Create a Zero copy clone one table

--1st-questions====================================
create or replace table q1_table(
    id integer primary key,
    name varchar(30)
);
Insert into q1_table values(1, 'shamb'),(2, 'saket'), (3, 'saransh');

drop table q1_table;
select * from q1_table;
undrop table q1_table;

--2nd question ===========================================

Delete from q1_table where id = 3;
Insert into q1_table values(4, 'satty'),  (6, 'aditya');
select * from q1_table;

-- if the table is truncated else it will create duplicacy
insert into q1_table SELECT * FROM q1_table BEFORE( STATEMENT => '01bcf38a-3201-b10f-000d-edee0002160a');

--- or ----
CREATE OR REPLACE TABLE q1_table_copy AS
    SELECT * FROM q1_table BEFORE( STATEMENT => '01bcf38a-3201-b10f-000d-edee0002160a');
select * from q1_table_copy;


truncate table q1_table;
Insert into q1_table select * from q1_table_copy; 

select * from q1_table;
drop table q1_table_copy;

--3rd question ========================================
CREATE OR REPLACE SEQUENCE my_sequence 
  start with 1 
  increment BY 1
--   MINVALUE 1
--   MAXVALUE 999999
--   CYCLE
  order;

  create or replace table q3_table(
    id integer default my_sequence.NEXTVAL,
    name varchar(30)
);
truncate table q3_table;
Insert into q3_table(name) values('shamb'),( 'saket'), ('saransh');

select * from q3_table;

-- 4th question ============================================

create or replace table source_table(
    order_id int primary key,
    order_name varchar(30),
    price number(10,2),
    order_date date
);
insert into source_table values(1, 'pasta', 333, current_date());
insert into source_table values(2, 'oata', 111, '2025-05-03'), (3, 'gajar', 30, '2025-05-03'),
                                (104, 'Noodles',  200, '2025-05-21'),(105, 'Fries', 120, '2025-04-28');
                                
update source_table set order_name = 'snowflake' where order_id = 2;

select * from source_table;
create or replace table target_table like source_table;
select * from target_table;
truncate table target_table;


------procedure --1-- for my_task_1

create or replace procedure insert_delete_data(source_table string, target_table string)
returns string
language sql
as
begin
    delete from identifier(:target_table) where order_id in (
         select order_id from identifier(:source_table)
            where order_date >=  date_trunc('month', dateadd(month, -1, current_date()))
            and order_date <  date_trunc('month', current_date())
            );

        insert into identifier(:target_table) select * from identifier(:source_table)
            where order_date >=  date_trunc('month', dateadd(month, -1, current_date()))
            and order_date <  date_trunc('month', current_date());

    return 'data deleted and inserted completely';
end;


-- procedure 2 for my_task_2
create or replace table target_table2 like source_table;
select * from target_table2;
truncate table target_table2;


create or replace procedure insert_update_data(source_table string, target_table2 string)
returns string
language sql
as
begin
    merge into identifier(:target_table2) as target
    using(select * from identifier(:source_table) 
    where order_date >=  date_trunc('month', dateadd(month, -1, current_date()))
            and order_date < date_trunc('month', current_date())
    ) as source
    on target.order_id = source.order_id
    when matched then
    update set
        order_name = source.order_name,
        price = source.price,
        order_date = source.order_date
    when not matched then
    insert (order_id, order_name, price, order_date ) values (source.order_id, source.order_name, source.price, source.order_date );

    return 'data insertion and updation completed';

end;
    

-- 5th question----------------
CREATE OR REPLACE TASK my_task_1
WAREHOUSE = COMPUTE_WH
SCHEDULE ='USING CRON 0 0 1 * * UTC'
    as
    call insert_delete_data('source_table', 'target_table');

alter task my_task_1 resume;
  -------------------------------
CREATE OR REPLACE TASK my_task_2
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 0 1 * * UTC'
    as
    call insert_update_data('source_table', 'target_table2');

      
show tasks;
alter task my_task_2 resume;
alter task my_task_2 suspend;

--6th question  =================================
create or replace table q5_table(
    id integer,
    name varchar(30)
);
Insert into q5_table values(1, 'shamb'),(2, 'saket'), (3, 'saransh');

create or replace  table clone_table clone q5_table; 
select * from clone_table;

-------------------------------------------------

select  date_trunc('month', dateadd(month, -3, current_date()));
select date_trunc('month', current_date());

select 
  current_date() as today,
  dateadd(month, -3, current_date()) as one_month_back,
  date_trunc('month', dateadd(month, -3, current_date())) as last_month_start;









