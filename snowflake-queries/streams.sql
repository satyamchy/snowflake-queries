use role accountadmin;
use warehouse compute_wh;
use schema mydb.myschema;

-- //== STANDARD STREAM ===
-- // create a source table
create or replace table source_table1(
    id int,
    name varchar(10),
    created_data DATE
);
insert into source_table1 values
    (1, 'sam', '2030-01-01'),
    (2, 'saket', '2010-01-01'),
    (3, 'shamb', '2015-01-01'),
    (4, 'adi', '2003-10-13');

-- //===create a standard stream on the table ===
create or replace stream standard_stream on table source_table1;

select * from source_table1;
select * from standard_stream;

Insert into source_table1 values
    (6, 'keshab', '2000-11-11');

select * from source_table1;
select * from standard_stream;

delete from source_table1 where id  = 2; 
update source_table1 set name = 'shivamm' where id = 6;

select * from source_table1;
select * from standard_stream;




-- == ================================================

-- //== APPEND ONLY STREAM ===
-- // create a source table
create or replace table source_table2(
    id int,
    name varchar(10),
    created_data DATE
);
insert into source_table2 values
    (1, 'sam', '2030-01-01'),
    (2, 'saket', '2010-01-01'),
    (3, 'shamb', '2015-01-01'),
    (4, 'adi', '2003-10-13');

-- //===create a standard stream on the table ===
create or replace stream append_only_stream on table source_table2  append_only = true;

select * from source_table2;
select * from append_only_stream;

Insert into source_table2 values
    (6, 'keshab', '2000-11-11');

select * from source_table2;
select * from append_only_stream;

delete from source_table2 where id  = 2; 
update source_table2 set name = 'shivamm' where id = 6;
-- // only keeps update on append only

select * from source_table2;
select * from append_only_stream;





-- == ================================================

-- // how do we use the stream in ETL Process 

create or replace table TARGET_TABLE1(
    id int,
    name varchar(30),
    created_data DATE
);

select * from append_only_stream;
insert into TARGET_TABLE1 
    select id, name, created_data from append_only_stream;

select * from append_only_stream;
select * from TARGET_TABLE1;

Insert into source_table2 values
    (7, 'alpha', '2222-11-11');

select * from source_table2;
select * from append_only_stream; // dml action happened on stream so data moved to target_table1;
select * from TARGET_TABLE1;

-- ========================
-- //  insert only stream
create external table ext_table
    location = @my_aws_stage
    file_format = my_format;

create  stream my_external_stream
    on external table ext_table
    insert_only = true;

t