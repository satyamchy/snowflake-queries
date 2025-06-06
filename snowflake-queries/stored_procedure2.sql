create function <name> (
    --<arg_name> <arg_data_type>
    --, ...
    )
    returns { <result_data_type> | table ( <col_name> <col_data_type> [ , ...] ) }
    -- language javascript
    -- { called on null input | { returns null on null input | strict } } ]
    -- volatile | immutable
    -- comment = '<comment>'
    as $$<function_definition>$$;

-----------------------------------------------------------------------------------------
GRANT USAGE ON SCHEMA sample_schema TO ROLE sam;
GRANT CREATE PROCEDURE ON SCHEMA sample_schema TO ROLE sam;
revoke CREATE PROCEDURE ON SCHEMA sample_schema from ROLE accountadmin;

DROP PROCEDURE IF EXISTS my_procedure(INT, STRING);
SHOW PROCEDURES IN SCHEMA my_database.my_schema;



create or replace table user(user_id int, user_name varchar);

----Insert Into Table
create or replace procedure add_user(id int, name string)
    returns string
    language SQL
as
$$
BEGIN
    insert into user(user_id, user_name)
    values(:id, :name);

    RETURN 'USER   added successfully';
END;
$$;

call add_user(1, 'sam');


-- common syntax for stored procedure in sql
declare var_name string;

--if - else in sql
BEGIN
  IF :value > 10 THEN
    RETURN 'Greater than 10';
  ELSE
    RETURN '10 or less';
  END IF;
END;


-- loops in js
for (var i = 0; i < 5; i++) {
  snowflake.execute({
    sqlText: `INSERT INTO my_table VALUES (${i})`
  });
}
--dynamic sql statement
var sql = `DELETE FROM my_table WHERE id = 100`;
var stmt = snowflake.createStatement({sqlText: sql});
stmt.execute();


--------------------------------------------------------------
create or replace procedure proc8(id int)
returns string
language sql
as
declare 
    c int; 
    namee string;
begin
  select count(*) into c from employees where employee_id = :id;
  if (c > 0) then 
  select first_name into namee from employees where employee_id = :id;
  return namee;
  else 
     return 'Employee does not exist';
  end if;
end;



call proc8(119);
select * from employees;
 
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
 
create or replace procedure cnt_emp( employees string)
returns int
language sql
as
declare 
    answer int default 0;
 begin
        -- No dynamic SQL needed; IDENTIFIER() lifts the table name safely
    select count(*) into :answer from IDENTIFIER(:employees); 
    -- bind variable as an object identifier
    return answer;     
end;

call cnt_emp('employees'); 


-----------------------------------------------------------------------------------------
create or replace procedure cnt_emp1( employees string)
returns int
language sql
declare 
    query string
    int result
as
    begin
    query := 'select count(*) from' || employees;
    execute immediate query into result;
    return result;
    end;

--what''s is the error
call cnt_emp1('employees'); 

-----------------------------------------------------------------------------------------


    