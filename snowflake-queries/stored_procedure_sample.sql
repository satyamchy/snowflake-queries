CREATE OR REPLACE PROCEDURE RAW_TO_TARGET_FULLDATA_REFRESH("SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216), "TARGET_SCHEMA" VARCHAR(16777216), "TARGET_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
    common_columns STRING;
    insert_statement STRING;
    backup_table_name VARCHAR;
    target_count INT;
    source_count INT;
    error_message VARCHAR(16777216);
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    duration varchar;
    sql_command string;
BEGIN
    SELECT LISTAGG(t.COLUMN_NAME, ',') 
    INTO common_columns
    FROM INFORMATION_SCHEMA.COLUMNS t
    JOIN INFORMATION_SCHEMA.COLUMNS s ON t.COLUMN_NAME = s.COLUMN_NAME
    WHERE t.TABLE_SCHEMA = :TARGET_SCHEMA
      AND t.TABLE_NAME = :TARGET_TABLE
      AND s.TABLE_SCHEMA = :SOURCE_SCHEMA
      AND s.TABLE_NAME = :SOURCE_TABLE
    ORDER BY t.ORDINAL_POSITION;
 
    backup_table_name := :TARGET_TABLE || '_BKP';
 
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TABLE ' || :TARGET_SCHEMA || '.' || backup_table_name || ' AS SELECT * FROM ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE;
 
   -- EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE;
    start_time:=current_timestamp();
    insert_statement := 'INSERT INTO ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE || ' (' || common_columns || ') SELECT ' || common_columns || ' FROM ' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE;
    EXECUTE IMMEDIATE insert_statement;
    end_time:=current_timestamp();
    duration := TO_VARCHAR(TIMESTAMPDIFF('second', :start_time, :end_time)) || ' seconds';
 
    EXECUTE IMMEDIATE 'CREATE TEMPORARY TABLE temp_count_table (count_col INT)';
    EXECUTE IMMEDIATE 'INSERT INTO temp_count_table SELECT COUNT(*) FROM ' || TARGET_SCHEMA || '.' || TARGET_TABLE;
    SELECT count_col INTO target_count FROM temp_count_table;
    EXECUTE IMMEDIATE 'DROP TABLE temp_count_table';
 
 
    EXECUTE IMMEDIATE 'CREATE TEMPORARY TABLE temp_count_table (count_col INT)';
    EXECUTE IMMEDIATE 'INSERT INTO temp_count_table SELECT COUNT(*) FROM ' || SOURCE_SCHEMA || '.' || SOURCE_TABLE;
    SELECT count_col INTO source_count FROM temp_count_table;
    EXECUTE IMMEDIATE 'DROP TABLE temp_count_table';
    sql_command := 'INSERT INTO raw_target_refresh_audit_log ' ||
                   'SELECT ' || 
                   '''' || SOURCE_SCHEMA || '''' || ', ' ||
                   '''' || SOURCE_TABLE || '''' || ', ' ||
                   '''' || TARGET_SCHEMA || '''' || ', ' ||
                   '''' || TARGET_TABLE || '''' || ', ' ||
                   '''' || start_time || '''' || ', ' ||
                   '''' || end_time || '''' || ', ' ||
                   '''' || duration || '''' || ', ' ||
                   '''' || source_count || '''' || ', ' ||
                   '''' || target_count || '''' || 
                   ';';
  EXECUTE IMMEDIATE sql_command;
    IF (target_count = source_count) THEN
        --EXECUTE IMMEDIATE 'TRUNCATE table if exists ' || :TARGET_SCHEMA || '.' || TARGET_TABLE;     
        EXECUTE IMMEDIATE 'INSERT INTO ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE || ' SELECT * FROM ' || :TARGET_SCHEMA || '.' || backup_table_name;
    ELSE
        RETURN 'DATA INSERTED SUCCESSFULLY';
    END IF;
 
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS ' || :TARGET_SCHEMA || '.' || backup_table_name;
    END;







USE SCHEMA TARGET_CORE_WORK;
CREATE OR REPLACE PROCEDURE LAYER_TO_LAYER_INCREMENTAL_MERGE("SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216), "UNIQ_KEY" VARCHAR(16777216), "TARGET_SCHEMA" VARCHAR(16777216), "TARGET_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
    common_columns VARCHAR;
    delete_statement VARCHAR;
    insert_statement VARCHAR;
    drop_statement VARCHAR;
	target_count INT;
    source_count INT;
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    duration varchar;
    sql_command varchar;
BEGIN
    start_time:=current_timestamp();
    SELECT LISTAGG(t.COLUMN_NAME, ',') 
    INTO :common_columns
    FROM INFORMATION_SCHEMA.COLUMNS t
    JOIN INFORMATION_SCHEMA.COLUMNS s ON t.COLUMN_NAME = s.COLUMN_NAME
    WHERE t.TABLE_SCHEMA = :TARGET_SCHEMA
      AND t.TABLE_NAME = UPPER(:TARGET_TABLE)
      AND s.TABLE_SCHEMA = :SOURCE_SCHEMA
      AND s.TABLE_NAME = UPPER(:SOURCE_TABLE)
    ORDER BY t.ORDINAL_POSITION;

 delete_statement := 'DELETE FROM ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE || ' WHERE (' || :UNIQ_KEY || ') IN (SELECT ' || :UNIQ_KEY || ' FROM ' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE || ')';

  
 EXECUTE IMMEDIATE delete_statement;

 insert_statement := 'INSERT INTO ' || :TARGET_SCHEMA || '.' || :TARGET_TABLE || ' (' || :common_columns || ') SELECT ' || :common_columns || ' FROM ' || :SOURCE_SCHEMA || '.' || :SOURCE_TABLE||' ; ';
 
EXECUTE IMMEDIATE   insert_statement;   
  end_time:=current_timestamp();
  duration := TO_VARCHAR(TIMESTAMPDIFF('second', :start_time, :end_time)) || ' seconds';
   
    EXECUTE IMMEDIATE 'CREATE TEMPORARY TABLE temp_count_table (count_col INT)';
    EXECUTE IMMEDIATE 'INSERT INTO temp_count_table SELECT COUNT(*) FROM ' || TARGET_SCHEMA || '.' || TARGET_TABLE;
    SELECT count_col INTO target_count FROM temp_count_table;
    EXECUTE IMMEDIATE 'DROP TABLE temp_count_table';


    EXECUTE IMMEDIATE 'CREATE TEMPORARY TABLE temp_count_table (count_col INT)';
    EXECUTE IMMEDIATE 'INSERT INTO temp_count_table SELECT COUNT(*) FROM ' || SOURCE_SCHEMA || '.' || SOURCE_TABLE;
    SELECT count_col INTO source_count FROM temp_count_table;
    EXECUTE IMMEDIATE 'DROP TABLE temp_count_table';
    sql_command := 'INSERT INTO raw_target_refresh_audit_log ' ||
                   'SELECT ' || 
                   '''' || SOURCE_SCHEMA || '''' || ', ' ||
                   '''' || SOURCE_TABLE || '''' || ', ' ||
                   '''' || TARGET_SCHEMA || '''' || ', ' ||
                   '''' || TARGET_TABLE || '''' || ', ' ||
                   '''' || start_time || '''' || ', ' ||
                   '''' || end_time || '''' || ', ' ||
                   '''' || duration || '''' || ', ' ||
                   '''' || source_count || '''' || ', ' ||
                   '''' || target_count || '''' || 
                   ';';

    EXECUTE IMMEDIATE sql_command;
	EXCEPTION
    WHEN OTHER THEN
       RETURN  ERROR_MESSAGE();
END;






CREATE OR REPLACE PROCEDURE DROP_AND_RECREATE_AS_CLONE_MONTHLY()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
    current_month VARCHAR;
	--last_month VARCHAR;
    SOURCE_DB VARCHAR  DEFAULT 'SOURCE';
    TARGET_DB VARCHAR  DEFAULT 'TARGET';
    SOURCE_SCHEMA VARCHAR  DEFAULT 'SOURCE_CORE';
    TARGET_SCHEMA VARCHAR  DEFAULT 'TARGET_CORE';
    TAB_ARRAY ARRAY DEFAULT ARRAY_CONSTRUCT('FACT_TABLE');
BEGIN
    --Get the current month and last month in 'MON' format
    current_month := TO_CHAR(CURRENT_DATE(), 'MON');
	--last_month := TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE()), 'MON');

   FOR I IN 0 TO ARRAY_SIZE(TAB_ARRAY) -1 DO
    -- Drop LAST_MONTH Table and recreate the clone for CURRENT table
    EXECUTE IMMEDIATE 
        ' DROP TABLE IF EXISTS '||:TARGET_DB || '.' || :TARGET_SCHEMA ||'.'|| TAB_ARRAY[I] ||'_CLONE';
    EXECUTE IMMEDIATE 
        ' CREATE TABLE '|| :TARGET_DB || '.'|| :TARGET_SCHEMA ||'.' || TAB_ARRAY[I] || '_CLONE' || ' CLONE ' || :SOURCE_DB || '.' ||:SOURCE_SCHEMA ||'.' || TAB_ARRAY[I];
    END FOR;
    -- Return success message
    RETURN 'Last month FACT_TABLE clone dropped and new FACT_TABLE clone created for ' || :current_month;


   EXCEPTION 
    WHEN OTHER THEN
        Return 'Failure during DROP_AND_RECREATE_AS_CLONE_MONTHLY:' || 'SQLERRM';


END;

can you explain this stored procedure in detail in step wise