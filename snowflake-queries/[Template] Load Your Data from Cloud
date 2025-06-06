/*--
In this Worksheet we will walk through templated SQL for the end to end process required
to load data from Amazon S3, Microsoft Azure and Google Cloud into a table.

    Helpful Snowflake Documentation:
        1. Bulk Loading from Amazon S3 - https://docs.snowflake.com/en/user-guide/data-load-s3
        2. Bulk Loading from Microsoft Azure - https://docs.snowflake.com/en/user-guide/data-load-azure
        3. Bulk Loading from Google Cloud Storage - https://docs.snowflake.com/en/user-guide/data-load-gcs
--*/


-------------------------------------------------------------------------------------------
    -- Step 1: To start, let's set the Role and Warehouse context
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- USE WAREHOUSE: https://docs.snowflake.com/en/sql-reference/sql/use-warehouse
-------------------------------------------------------------------------------------------

--> To run a single query, place your cursor in the query editor and select the Run button (⌘-Return).
--> To run the entire worksheet, select 'Run All' from the dropdown next to the Run button (⌘-Shift-Return).

---> set Role Context
USE ROLE ACCOUNTADMIN;

---> set Warehouse Context
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
USE DATABASE SNOWFLAKE_LEARNING_DB;

---> set the Schema
SET user_name = current_user();
SET schema_name = CONCAT($user_name, '_LOAD_DATA_FROM_CLOUD');
USE SCHEMA IDENTIFIER($schema_name);


-------------------------------------------------------------------------------------------
    -- Step 2: Create Table
        -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

---> create the Table
CREATE [ OR REPLACE ] TABLE [ IF NOT EXISTS ] <table_name>
    (
    <col1_name> <COL1_TYPE>
    ,<col2_name> <COL2_TYPE>
    --> supported types: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
    )
    [COMMENT = '<string_literal>'];

---> query the empty Table
SELECT * FROM <table_name>;


-------------------------------------------------------------------------------------------
    -- Step 3: Create Storage Integrations
        -- CREATE STORAGE INTEGRATION: https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
-------------------------------------------------------------------------------------------

    /*--
      A Storage Integration is a Snowflake object that stores a generated identity and access management
      (IAM) entity for your external cloud storage, along with an optional set of allowed or blocked storage locations
      (Amazon S3, Google Cloud Storage, or Microsoft Azure).
    --*/

---> Create the Amazon S3 Storage Integration
    -- Configuring a Snowflake Storage Integration to Access Amazon S3: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

CREATE [ OR REPLACE ] STORAGE INTEGRATION [ IF NOT EXISTS ] <s3_integration_name>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = '<iam_role>'
  [ STORAGE_AWS_OBJECT_ACL = 'bucket-owner-full-control' ]
  ENABLED = { TRUE | FALSE }
  STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/<path>/' [ , 's3:://<bucket>/<path>/' ... ] )
  [ STORAGE_BLOCKED_LOCATIONS = ('s3:://<bucket>/<path>/' [ , 's3:://<bucket>/<path>/' ... ] ) ]
  [ COMMENT = '<string_literal>' ];

    /*--
      Execute the command below to retrieve the ARN and External ID for the AWS IAM user that was created automatically for your Snowflake account.
      You’ll use these values to configure permissions for Snowflake in your AWS Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration#step-5-grant-the-iam-user-permissions-to-access-bucket-objects
    --*/

---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration
DESCRIBE INTEGRATION <s3_integration_name>;

---> Create the Microsoft Azure Storage Integration
    -- Configuring an Azure Container for Loading Data: https://docs.snowflake.com/en/user-guide/data-load-azure-config

CREATE [ OR REPLACE ] STORAGE INTEGRATION [ IF NOT EXISTS ] <azure_integration_name>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  AZURE_TENANT_ID = '<tenant_id>'
  ENABLED = { TRUE | FALSE }
  STORAGE_ALLOWED_LOCATIONS = ('azure://<bucket>/<path>/' [ , 'azure:://<bucket>/<path>/' ... ] )
  [ STORAGE_BLOCKED_LOCATIONS = ('azure:://<bucket>/<path>/' [ , 'azure:://<bucket>/<path>/' ... ] ) ]
  [ COMMENT = '<string_literal>' ];

    /*--
      Execute the command below to retrieve the AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME for the client application created
      automatically for your Snowflake account. You’ll use these values to configure permissions for Snowflake in your Azure Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-azure-config#step-2-grant-snowflake-access-to-the-storage-locations
    --*/

---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration

DESCRIBE INTEGRATION <azure_integration_name>;

---> Create the Google Cloud Storage Integration
    -- Configuring an Integration for Google Cloud Storage: https://docs.snowflake.com/en/user-guide/data-load-gcs-config

CREATE [ OR REPLACE ] STORAGE INTEGRATION [ IF NOT EXISTS ] <gcs_integration_name>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = { TRUE | FALSE }
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<bucket>/<path>/' [ , 'gcs://<bucket>/<path>/' ... ] )
  [ STORAGE_BLOCKED_LOCATIONS = ('gcs://<bucket>/<path>/' [ , 'gcs://<bucket>/<path>/' ... ] ) ]
  [ COMMENT = '<string_literal>' ];

    /*--
      Execute the command below to retrive the ID for the Cloud Storage Service Account that was created automatically for your Snowflake account.
      You’ll use these values to configure permissions for Snowflake in your GCP Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-gcs-config#step-2-retrieve-the-cloud-storage-service-account-for-your-snowflake-account
    --*/

---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration
DESCRIBE INTEGRATION <gcs_integration_name>;


---> View our Storage Integrations
    -- SHOW INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/show-integrations

SHOW STORAGE INTEGRATIONS;


-------------------------------------------------------------------------------------------
    -- Step 6: Create Stage Objects
-------------------------------------------------------------------------------------------

    /*--
      A stage specifies where data files are stored (i.e. "staged") so that the data in the files
      can be loaded into a table.
    --*/

---> Create the Amazon S3 Stage
    -- Creating an S3 Stage: https://docs.snowflake.com/en/user-guide/data-load-s3-create-stage

CREATE [ OR REPLACE ] STAGE [ IF NOT EXISTS ] <s3_stage_name>
URL = { 's3://<bucket>[/<path>/]' | 's3://<bucket>[/<path>/]' }
STORAGE_INTEGRATION = <s3_integration_name> -- created in previous step
[ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
[ COMMENT = '<string_literal>' ];


---> Create the Microsoft Azure Stage
    -- Creating an Azure Stage: https://docs.snowflake.com/en/user-guide/data-load-azure-create-stage

CREATE [ OR REPLACE ] STAGE [ IF NOT EXISTS ] <azure_stage_name>
URL = { 'azure://<bucket>[/<path>/]' | 'azure://<bucket>[/<path>/]' }
STORAGE_INTEGRATION = <azure_integration_name> -- created in previous step
[ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
[ COMMENT = '<string_literal>' ];


---> Create the Google Cloud Storage Stage
    -- Create a Google Cloud Stage: https://docs.snowflake.com/en/user-guide/data-load-gcs-config#create-an-external-stage-using-sql

CREATE [ OR REPLACE ] STAGE [ IF NOT EXISTS ] <gcp_stage_name>
URL = { 'gcs://<bucket>[/<path>/]' | 'gcs://<bucket>[/<path>/]' }
STORAGE_INTEGRATION = <gcp_integration_name> -- created in previous step
[ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
[ COMMENT = '<string_literal>' ];


---> View our Stages
    -- SHOW STAGES: https://docs.snowflake.com/en/sql-reference/sql/show-stages

SHOW STAGES;


-------------------------------------------------------------------------------------------
    -- Step 7: Load Data from Stages
-------------------------------------------------------------------------------------------

---> Load data from the Amazon S3 Stage into the Table
    -- Copying Data from an S3 Stage: https://docs.snowflake.com/en/user-guide/data-load-s3-copy
    -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

COPY INTO <table_name>
  FROM @<s3_stage_name>
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ];

---> Load data from the Azure Stage into the Table
    -- Copying Data from an Azure Stage: https://docs.snowflake.com/en/user-guide/data-load-azure-copy
    -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

COPY INTO <table_name>
  FROM @<azure_stage_name>
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ];

---> Load data from the Google Cloud Stage into the Table
    -- Copying Data from a Google Cloud Storage Stage: https://docs.snowflake.com/en/user-guide/data-load-gcs-copy
    -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

COPY INTO <table_name>
  FROM @<gcp_stage_name>
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ];


-------------------------------------------------------------------------------------------
    -- Step 8: Start querying your Data!
-------------------------------------------------------------------------------------------

---> Great job! You just successfully loaded data from your cloud provider into a Snowflake table
---> through an external stage. You can now start querying or analyzing the data.

SELECT * FROM <table_name>;
