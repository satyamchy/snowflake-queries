
-- Create User for Informatica
use role USERADMIN;
create user INFAUSER password = 'YourSecurePassword' 
  COMMENT = 'User to connect from Informatica application' 
  MUST_CHANGE_PASSWORD = FALSE;

show roles;
show users;

 
-- Grant necessary roles
GRANT ROLE accountadmin TO USER INFAUSER;
revoke role accountadmin from user infauser;

-- Grant usage on database and schema
create role customrole;
GRANT SELECT ON ALL TABLES IN SCHEMA mydb.myschema TO ROLE customrole;
GRANT role customrole TO USER INFAUSER;
GRANT USAGE ON SCHEMA your_database.your_schema TO USER INFAUSER;

-- Grant privileges on tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA your_database.your_schema TO USER INFAUSER;

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE your_warehouse TO USER INFAUSER;

--//  
show grants on warehouse compute_wh;
grant usage on warehouse compute_wh to role orgadmin;

grant role orgadmin to user satyam19113;
grant role orgadmin to user sam;
grant all on database mydb to role satyam19113;


----------------------------------------------------------------------------



create role developer;

grant usage on database mydb to role developer;
revoke usage on database my_db to role developer;

select * from SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY;

show tables;

--------------------------------------------------------

CREATE ROLE my_app_role;
GRANT USAGE ON DATABASE my_db TO ROLE my_app_role;
GRANT SELECT ON TABLE my_table TO ROLE my_app_role;







