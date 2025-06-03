use role accountadmin;
use warehouse compute_wh;
use schema mydb.myschema;
--create or replace database mydb;

-- === ===  TIME TRAVEL( DELETING SOME ROWS ) === 

truncate table parrot;
create or replace table parrot(
    id integer primary key,
    name varchar(30)
);
Insert into parrot values(1, 'shamb'),(2, 'saket'), (3, 'saransh');

select * from parrot;
Delete from parrot where id = 3;
Insert into parrot values(4, 'satty');


SELECT * FROM parrot AT( OFFSET => -1*60*24);
SHOW TABLES; -- timing of the table creation

SELECT * FROM parrot AT( TIMESTAMP => '');
SELECT * FROM parrot AT( TIMESTAMP => '2025-05-13 01:46:14.812 -0700' :: timestamp_tz);


SELECT * FROM parrot BEFORE( STATEMENT => '01bc9a42-3201-a7bf-000d-4d1e00068166'); -- id of the query
SELECT * FROM parrot BEFORE( STATEMENT => '01bc5149-3201-9d79-000d-4d1e00020a3a');


CREATE OR REPLACE TABLE CLONE_parrot_TABLE AS
    SELECT * FROM parrot BEFORE( STATEMENT => '01bc5149-3201-9d79-000d-4d1e00020a3a');
    
CREATE TABLE  restored_parrot  
    CLONE parrot AT ( TIMESTAMP => '2025-05-20 10:00:00'); 

Insert into parrot select * from CLONE_parrot_TABLE; 

select * from parrot;
drop table CLONE_parrot_TABLE;

select * from CLONE_parrot_TABLE;

-- ===== Dropping table, dataabase, schemas  ==== 

drop table parrot;
select * from parrot;
undrop table parrot;

drop schema myschema;
undrop schema myschema;

drop database MYDB;
undrop database MYDB;


use schema mydb.myschema;

SHOW DATABASES;
SHOW SCHEMAS;
SHOW TABLES;

ALTER TABLE parrot SET DATA_RETENTION_TIME_IN_DAYS = 11;


--======= ZERO COPY CLONNING ============ =================================
--================================================================================

CREATE DATABASE clone_db CLONE MYDB; 
DROP DATABASE CLONE_DB;

CREATE TABLE copy_table CLONE original_table; 









--===========================================================
-- Concept	Example	Meaning
-- UTC (Coordinated Universal Time)	UTC+0	The global time standard
-- Offset	-0700	7 hours behind UTC
-- Time Zone Code	PST, EST, IST	Named time zones (based on regions)

-- Snowflake supports 3 types of timestamps:
-- Type	Description
-- TIMESTAMP_NTZ	No time zone (interpreted as session time zone)
-- TIMESTAMP_LTZ	Local time zone (uses current session’s zone)
-- TIMESTAMP_TZ	    Includes a time zone offset

---- Set session to a specific time zone
ALTER SESSION SET TIMEZONE = 'America/Los_Angeles';
SELECT
  CURRENT_TIMESTAMP() AS utc_time,
  CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP()) AS india_time;














