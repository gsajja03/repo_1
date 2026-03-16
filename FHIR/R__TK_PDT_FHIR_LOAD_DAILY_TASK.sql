CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_BIG_WH
SCHEDULE  = 'USING CRON 0 20 * * * America/Chicago'
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   COPY INTO {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR FROM 
        (     SELECT 
                  METADATA$FILENAME , 
                  METADATA$FILE_ROW_NUMBER , 
                  METADATA$FILE_CONTENT_KEY , 
                  METADATA$FILE_LAST_MODIFIED , 
                  METADATA$START_SCAN_TIME ,
                  NVL(PARSE_JSON($1):resourceType , ' '),
                  NVL(PARSE_JSON($1):id , ' ') , 
                  NVL($1,' ') ,
                  CURRENT_DATE()
             FROM @{{env}}_RAW_INBOUND_DB.BCBSA.DIH_EXCG_EXTERNAL_INBOUND_BCBSA_STAGE/FHIR/
             (file_format => {{env}}_RAW_INBOUND_DB.BCBSA.FHIR_JSON_FORMAT)t ) PATTERN= '.*[.]ndjson' ON_ERROR =CONTINUE;

   END;

   ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK RESUME;