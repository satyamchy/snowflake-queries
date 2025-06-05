use database mydb;
use schema newschema;

create or replace table semistructured_data( data variant );
select * from semistructured_data;

--// sample file format
create or replace file format temp_ff
    type = 'csv'
    field_delimiter = ','
    skip_header = 1
    field_optionally_inclosed_by = '"'
    on_error = 'continue';
    
--// sample storage integration
create or replace storage integration temp_storage_int
    type = external_stage
    storage_provider = 's3'
    enabled = true
    storage_aws_role_arn = ''
    storage_allowed_locations = ('')

desc temp_storage_int;
    
--=11111111======================================================================================================================
-- Export Snowflake table from Snowflake Sample DB data to csv file in local disk using GET command 
-- Export Snowflake table from Snowflake Sample DB data to multiple csv files based on file size(10MB) in local disk using GET command 
-- Export Snowflake table from Snowflake Sample DB data to AWS S3 to multiple files
--****************************************************************************************************************************

copy into @stage/temp_stage
    from temp_table
    file_format = temp_fileformat;



    
--222222222=======================================================================================================================
-- 5   There is snowflake table and the data gets loaded (Insert and Update) into this table regularly. Due to an error, few columns are updated with incorrect values. 
-- 1. To avoid this, enable snowflake table to hold history of the data and write queries to get the correct data and make the table stable.
-- 2. This table gets updated daily, write a query to get the data from this table with given point time.
---****************************************************************************************************************************
alter table snowflake_table set DATA_RETENTION_TIME_IN_DAYS = 30;

create table temp_snowflake_table 
    as select * from snowflake_table before(STATEMENT => 'query_id' );
create table restored_snowflake_table 
    clone snowflake_table at(timestamp => '2025-05-20 10:00:00');

truncate table snowflake_table;
insert into snowflake_table select * from temp_snowflake_table;

select * from snowflake_table at ( offset => '-60 * 24');
select * from snowflake_table at ( timestamp => '');







--3333333333333=======================================================================================================================
-- 6  Implement continous loading which captures insert, updates and deletes of the source table to target table with time interval of 5 mins. 
-- Source: Orders
-- Target: Fact_orders
--****************************************************************************************************************************
create or replace table source_orders_table (
    id int,
    order_name varchar(30),
    price float,
    date Date
);
-- (source table -RDS) -><- (tracked by stream) -> (update warehouse)
create or replace stream cdc_stream_table on table source_orders_table ;
create or replace table  target_fact_orders_table like source_orders_table;

insert into source_orders_table values(1, 'cheese', 100, '2025-06-04');
delete from source_orders_table where id = 1;
update  source_orders_table set  order_name = 'ghee' where id = 1;
insert into source_orders_table values(2, 'cat', 500, '2025-06-04');
insert into source_orders_table values(3, 'milk', 62, '2025-06-04');


create or replace task task_q2
    warehouse = 'compute_wh'
    schedule = '1 MINUTES'
    as

    --call process_cdc_stream_task(); 
    
     merge into target_fact_orders_table as target 
     using( select * from cdc_stream_table
     ) as source on target.id = source.id
     when matched and source.METADATA$ACTION = 'DELETE' then delete
     when matched then 
     update set
     target.id = source.id,
     target.order_name = source.order_name,
     target.price = source.price,
     target.date = source.date
     when not matched then insert(id, order_name, price, date) 
     values(source.id, source.order_name, source.price,   source.date);

-- delete , update(delete-> insert), insert so only 2 conditions
show tasks;
alter task task_q2 resume;
alter task task_q2 suspend;

select * from source_orders_table;
select * from cdc_stream_table;
select * from target_fact_orders_table;

truncate table source_orders_table;
truncate table target_fact_orders_table;

------------------------------------------------------------
DELIMITER $$

create or replace procedure process_cdc_stream_task()
returns string 
language sql
as
---$$
BEGIN
    
    delete from target_fact_orders_table where id in(
    select id from cdc_stream_table where METADATA$ACTION = 'DELETE'
    );
    
    insert into target_fact_orders_table (id, order_name, price, date) 
         select id, order_name, price, date 
         from cdc_stream_table 
         where METADATA$ACTION = 'INSERT';

    RETURN 'CDC processing completed.';
END $$
DELIMITER ;
--$$;


--44444444444== don.=====================================================================================================================
-- 7	Insert a CSV file from AWS with continuous loading:

-- 1) Whenever a new file is added to AWS S3 path, should be loaded to Snowflake
-- 2) Schedule this process to load files daily
-- 3) Enable an option to manually run the process to load files to Snowflake
--****************************************************************************************************************************
-----loading data manually
create or replace stage temp_stage
    url = ''
    storage_integraion = temp_storage_int
    file_format = temp_ff;
------snowpipe to load data---------------------------------------
create or replace pipe continuous_loading_pipe
    auto ingest = true
    as
    copy into temp_table_1 
    from @temp_stage 
    file_format = temp_ff;
    
show pipes;  -- sqs arn availble and to be added to s3Bucket notification channel for notification
select SYSTEM$PIPE_STATUS('continuous_loading_pipe');
select * from temp_table_1;

------using task to load file to table-------------------------------
create or replace task temp_task_4
    warehouse = 'warehouse_name'
    schedule = 'using cron 0 0 * * * UTC'
    as
    copy into temp_table_2
    from @temp_stage
    file_format = temp_ff;
    
alter task temp_task_4 resume;
alter task temp_task_4 suspend;
select * from temp_table_4;

--------manually loading the file to snowflake-------------------------  
copy into temp_table_4
    from @temp_stage
    file_format = temp_ff;
    
--=======================================================================================================================

