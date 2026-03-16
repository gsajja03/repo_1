/*USE DATABASE DEV_RAW_INBOUND_DB;
USE SCHEMA CEDAR_GATE_RESP;
SELECT COUNT(*) FROM DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.MEMBER_INCREMENTAL;
DELETE FROM DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.PARTICIPATION_INCREMENTAL;
UPDATE  COMMON.TABLE_TO_TABLE_LOAD_CONFIG SET  RUN_BACKUP_TRIGGER_IND = TRUE  WHERE SOURCE_DB_NAME = 'DEV_AR_INBOUND_DB' AND 
SOURCE_TABLE_NAME IN ('MEMBER','PARTICIPATION');
SELECT * FROm DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL;
UPDATE DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL SET ARCHIVE_STATUS = FALSE WHERE LOAD_DT = '04/15/2025' AND TABLE_NAME IN ('MEMBER','PARTICIPATION');
INSERT INTO DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL (IMPORT_DATE, PROCESSING_TICKET, TABLE_NAME, IMPORT_STATUS, METADATA_COUNT, IMPORT_COUNT, COUNT_VERIFICATION_STATUS, ARCHIVE_STATUS, LOAD_DT)
SELECT IMPORT_DATE, PROCESSING_TICKET, TABLE_NAME, IMPORT_STATUS, METADATA_COUNT, IMPORT_COUNT, COUNT_VERIFICATION_STATUS, ARCHIVE_STATUS, LOAD_DT FROM PRD_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL;

SELECT * FROM DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL WHERE ARCHIVE_STATUS = FALSE;

SELECT * FROM COMMON.TABLE_TO_TABLE_LOAD_CONFIG  WHERE SOURCE_DB_NAME = 'DEV_AR_INBOUND_DB' AND  RUN_BACKUP_TRIGGER_IND = TRUE  AND SOURCE_TABLE_NAME IN ('MEMBER','PARTICIPATION');

CALL CEDAR_GATE_RESP.CEDAR_GATE_MASTR_FEEDBACK_PARALLEL_TABLE_LOAD_SP();*/


CREATE OR REPLACE PROCEDURE CEDAR_GATE_RESP.CEDAR_GATE_MASTR_FEEDBACK_PARALLEL_TABLE_LOAD_SP()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE 

C1 CURSOR FOR SELECT SOURCE_DB_NAME,SOURCE_SCHEMA_NAME,SOURCE_TABLE_NAME,TARGET_DB_NAME,TARGET_SCHEMA_NAME,TARGET_TABLE_NAME,LOAD_TYPE,FREQUENCY  FROM COMMON.TABLE_TO_TABLE_LOAD_CONFIG WHERE RUN_BACKUP_TRIGGER_IND = TRUE;
    V_TARGET_DB_NAME         STRING;
    V_TARGET_SCHEMA_NAME      STRING;
    V_TARGET_TABLE_NAME       STRING;
    V_SOURCE_DB_NAME          STRING;
    V_SOURCE_SCHEMA_NAME      STRING;
    V_SOURCE_TABLE_NAME       STRING;
    V_LOAD_TYPE               STRING;


BEGIN
 --STEP #1 Loop Through Each Configuration Row Where We Need To Trigger A Load
  
FOR record IN C1
  DO

    V_TARGET_DB_NAME          := RECORD.TARGET_DB_NAME       ;
    V_TARGET_SCHEMA_NAME      := RECORD.TARGET_SCHEMA_NAME   ;
    V_TARGET_TABLE_NAME       := RECORD.TARGET_TABLE_NAME    ;
    V_SOURCE_DB_NAME          := RECORD.SOURCE_DB_NAME       ;
    V_SOURCE_SCHEMA_NAME      := RECORD.SOURCE_SCHEMA_NAME   ;
    V_SOURCE_TABLE_NAME       := RECORD.SOURCE_TABLE_NAME    ;
    V_LOAD_TYPE               := RECORD.LOAD_TYPE            ;



ASYNC(  CALL CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP(
        :V_TARGET_DB_NAME,
        :V_TARGET_SCHEMA_NAME,
        :V_TARGET_TABLE_NAME,
        :V_SOURCE_DB_NAME,
        :V_SOURCE_SCHEMA_NAME,
        :V_SOURCE_TABLE_NAME,
        :V_LOAD_TYPE )
        );
 
END FOR;
 
--Wait for :parallel_job_id1 job to complete
 AWAIT ALL;
 
RETURN 'Cedar Gate Table Archival is Complete';
 
END;
 
$$;



CALL CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP(
        'PRD_RAW_INBOUND_DB',
        'CEDAR_GATE_RESP',
        'DENTAL_CLAIMS',
        'PRD_AR_INBOUND_DB',
        'CEDAR_GATE',
        'DENTAL_CLAIMS',
        'APPEND' );
        

        

        +-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP                                                                                                                                                               |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Failed: Code: 2043                                                                                                                                                                              |
|   State: 02000                                                                                                                                                                                  |
|   Message: SQL compilation error:                                                                                                                                                               |
| Object does not exist, or operation cannot be performed.                                                                                                                                        |
| Stack Trace:                                                                                                                                                                                    |
| Statement.execute, line 53 position 37
INSERT INTO PRD_RAW_INBOUND_DB.CEDAR_GATE_RESP.DENTAL_CLAIMS(SELECT *, CURRENT_DATE() FROM PRD_AR_INBOUND_DB.PRD_AR_INBOUND_DB.CEDAR_GATE.DENTAL_CLAIMS LIMIT 100); |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 Row(s) produced. Time Elapsed: 0.663s
SFK_SVC_PRD_AB_ELT#PRD_ELT_STD_WH@PRD_RAW_INBOUND_DB.CEDAR_GATE_RESP>