use role accountadmin;
use warehouse compute_wh;
use schema mydb.myschema;

create or replace table s3_table(
    id number(10),
    name varchar(30),
    email varchar(30),
    gender varchar(1),
    location varchar(30)
);

desc table s3_table;
show tables like 's3_table';

----------------------------------------------------------------------------
-- create a storage integration with s3 and iam role
create or replace STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::992382567849:role/rsaggarwal'
    --'arn:aws:iam::847994851803:role/myRedshiftRole'
    STORAGE_ALLOWED_LOCATIONS = ('s3:--asfhkjasbfjkmasasdfa');

-- DESCRIBE the storage integration
DESC INTEGRATION s3_int;   
-- STORAGE_AWS_IAM_USER_ARN -- replace this id in iam-role-trust-relationship-edit-polity-aws-''  
--and external-id too in aws

-- unable to edit trust relationship policy in iam role college aws account

-- create a file format or replace
create or replace file format my_s3_data_csv_format
    type = 'csv'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1;

-- create an external s3 stage
create or replace  STAGE my_s3_stage
    storage_integration = s3_int
    url = 's3:--asfhkjasbfjkmasasdfa'
    file_format = my_s3_data_csv_format;

-- access the external stage
list @my_s3_stage;
-- User: arn:aws:iam::151098411136:user/i0e01000-s is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::847994851803:role/myRedshiftRole

-------------------------------------------------------------------------
-- load data into user1 table with file format
copy into s3_table 
    from @my_s3_stage
    file_format = (format_name = my_s3_data_csv_format);


-- select data from table
select * from s3_table;
truncate table s3_table;

select * from information_schema.load_history;
select * from information_schema.task;


--================================================================================
-----------------------------------------------------------------------
-- CONTINOUS DAATA LOADING -- BATCH PROCESSING

-- CREATE A EVENT TABLE
create or replace table EVENT(
    event variant
);
desc table EVENT;
show tables like 'EVENT';


create or replace table snowpipe_table(
    id number(10),
    name varchar(30),
    email varchar(30),
    gender varchar(1),
    location varchar(30)
);
desc table snowpipe_table;

----------------------------------------------
-- create a storage integration with s3 and iam role
--  Snowpipe is Snowflake’s continuous data ingestion tool
--  It automatically loads data into a table as soon as new files arrive in a stage (storage location).

 
create or replace STORAGE INTEGRATION s3_snowpipe_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::992382567849:role/snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3:--snowflake-int-bucket-satyam/event/');

--  DESCRIBE the storage integration
DESC INTEGRATION s3_snowpipe_int; 
-- STORAGE_AWS_IAM_USER_ARN -- replace this id in iam-role-trust-relationship-edit-polity-aws-''  and external-id too in aws


-- create a file format 
create or replace file format my_json_format
    type = 'json';

-- using previous file format  --- my_s3_data_csv_format

-- create an external s3 stage
create or replace STAGE my_s3_snowpipe_stage
    storage_integration = s3_snowpipe_int
    url = 's3:--snowflake-int-bucket-satyam/event/'
    file_format = my_s3_data_csv_format;

-- Access the external stage
list @my_s3_snowpipe_stage;  

-- create a snowpipe to load the event data from s3
create or replace pipe s3_pipe
    auto_ingest = true as
    copy into snowpipe_table 
    from @my_s3_snowpipe_stage
    file_format  = (format_name = my_s3_data_csv_format);

-- select the status of the pipe
select SYSTEM$PIPE_STATUS('S3_pipe');

alter pipe s3_pipe refresh;

-- get the notification channeel
show PIPES;

-- select data from the table
select * from  snowpipe_table;

truncate table snowpipe_table;



 
--===================================
--===================================================

-- pointer to s3
create stage my_s3_stage1
url = ''
credentials =( ''
aws_secret_key = '')
ENCRYPTION = (master_key = '*****')
FILE_FORMAT = my_csv_format;

copy into my_table_s3 
    from @my_s3_stage1
    pattern = '.*sales.*.csv.';

