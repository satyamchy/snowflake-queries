use role accountadmin;
use warehouse compute_wh;
use schema mydb.myschema;

--=====WITHOUT TASK====
 
-- // create a source table
create or replace table SOURCE_TABLE(
    id int,
    name varchar(10),
    created_date DATE
);
insert into SOURCE_TABLE values
    (1, 'sam', '2030-01-01'),
    (2, 'saket', '2010-01-01'),
    (3, 'shamb', '2015-01-01'),
    (4, 'adi', '2003-10-13');

SELECT * FROM SOURCE_TABLE;
-- //------------------------------------------------------
-- // create a target table
create or replace table TARGET_TABLE(
    id int,
    name varchar(10),
    created_date DATE,
    created_day varchar,
    created_month varchar,
    created_year varchar
);

INSERT INTO TARGET_TABLE
    SELECT 
    a.id,
    a.name,
    a.created_date,
    DAY(a.created_date) as created_day,
    MONTH(a.created_date) as created_month,
    YEAR(a.created_date) as created_year
    FROM SOURCE_TABLE a
    left join TARGET_TABLE b
    on a.id = b.id
    where b.id is null;

SELECT * FROM TARGET_TABLE;

insert into SOURCE_TABLE values
    (6, 'satty', '1111-11-11');

SELECT * FROM SOURCE_TABLE;
SELECT * FROM TARGET_TABLE;
-- // any updation in source table we have to update target table manually by running it again 

-- //==================================
-- // =======WITH TASKS==========

create or replace table TARGET_TABLE2(
    id int,
    name varchar(10),
    created_date DATE,
    created_day varchar,
    created_month varchar,
    created_year varchar
);

-- // INSERTING DATA INTO TARGET TABLE USING TASK
-- //
CREATE OR REPLACE TASK my_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
    AS
    INSERT INTO TARGET_TABLE2
        SELECT
        a.id,
        a.name,
        a.created_date,
        DAY(a.created_date) as created_day,
        MONTH(a.created_date) as created_month,
        YEAR(a.created_date) as created_year
        FROM SOURCE_TABLE a
        left join TARGET_TABLE2 b
        on a.id = b.id
        where b.id is null;``   
SHOW TASKS;
-- // ALTER THE TASK TO EXECUTE
alter task my_task resume;
alter task my_task suspend;

-- // check the status of the task
select * from table (information_schema.TASK_HISTORY(TASK_NAME => 'my_task'));

insert into SOURCE_TABLE values
    (7, 'parrot', '2222-10-10');

SELECT * FROM SOURCE_TABLE;
SELECT * FROM TARGET_TABLE2; 
