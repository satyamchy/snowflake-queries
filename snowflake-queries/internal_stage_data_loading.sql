create or replace database snow_db;

use database MYDB;
create or replace schema sample_schema;


--==================  Loading data from IINTERNAL sources ============

-- creating a table
create or replace table customer_csv(
    id number(10),
    name varchar(30),
    email varchar(30),
    gender varchar(1),
    location varchar(30)
);


select * from customer_csv;

desc table customer_csv;    --- we have to create itself firstly

--  create a file format
create or replace file format customer_csv_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
skip_header = 1;

Copy into customer_csv
   from @sample_stage
   file_format = customer_csv_ff
   purge = true
   on_error = 'Continue';
   
select * from customer_csv;

show tables;
show stages;
show views;

--== continuous data loading using snowpipe in internal stage ==========================

CREATE OR REPLACE TABLE my_table (
  id INT, 
  name STRING
);   --  	Destination for loaded data

CREATE OR REPLACE STAGE my_internal_stage;  -- Stores the uploaded data files

--  SnowSQL CLI or Snowflake UI to upload the file
--snowsql -q "PUT file://path/to/data.csv @my_internal_stage AUTO_COMPRESS=TRUE;"

--  Create a Snowpipe---
CREATE OR REPLACE PIPE my_snowpipe AS  -- Automates the load from stage to table
COPY INTO my_table
FROM @my_internal_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- Manually Trigger Snowpipe
ALTER PIPE my_snowpipe REFRESH;


select * from my_table;



=========================================================================
-- to load big data

create or replace file format customer_csv_big_ff
type = 'csv'
compression = 'none'
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '\042';  -- double quotes  
 
select * from customer_csv;

truncate table customer_csv;

-- copy big datas
-- copy command errors

-- ON ERROR = ÇONTINUE' || ABORT STATEMENT || SKIP || SKIP_LIMIT(SKIP_FILE_10)

Copy into customer_csv
   from @customer_csv
   file_format = customer_csv_big_ff
   purge = true
   on_error = 'Continue';





--==============================