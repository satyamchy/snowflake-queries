
--------------------------------------------------------


CREATE SHARE my_share;

GRANT USAGE ON DATABASE mydb TO SHARE my_share;
GRANT USAGE ON SCHEMA mydb.myschema TO SHARE my_share;
GRANT SELECT ON TABLE parrot TO SHARE my_share;
ALTER SHARE my_share ADD ACCOUNTS = 'bzhftsg.xe25852';


SELECT CURRENT_ACCOUNT();
SELECT CURRENT_REGION();
SHOW ACCOUNTS;
SHOW GRANTS TO SHARE my_share;

