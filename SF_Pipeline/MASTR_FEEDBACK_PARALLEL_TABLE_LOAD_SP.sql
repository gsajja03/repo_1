CREATE OR REPLACE PROCEDURE CEDAR_GATE_RESP.CEDAR_GATE_MASTR_FEEDBACK_PARALLEL_TABLE_LOAD_SP()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
DECLARE
 
    C1 CURSOR FOR 
        SELECT SOURCE_DB_NAME,
               SOURCE_SCHEMA_NAME,
               SOURCE_TABLE_NAME,
               TARGET_DB_NAME,
               TARGET_SCHEMA_NAME,
               TARGET_TABLE_NAME,
               LOAD_TYPE,
               FREQUENCY  
        FROM COMMON.TABLE_TO_TABLE_LOAD_CONFIG 
        WHERE RUN_BACKUP_TRIGGER_IND = TRUE;
 
    V_TARGET_DB_NAME         STRING;
    V_TARGET_SCHEMA_NAME     STRING;
    V_TARGET_TABLE_NAME      STRING;
    V_SOURCE_DB_NAME         STRING;
    V_SOURCE_SCHEMA_NAME     STRING;
    V_SOURCE_TABLE_NAME      STRING;
    V_LOAD_TYPE              STRING;
    V_FREQUENCY              STRING;
 
    V_LOG                    STRING := '';
    V_START_TIME             TIMESTAMP := CURRENT_TIMESTAMP();
    V_RECORD_COUNT           INTEGER := 0;
    V_ASYNC_JOB_COUNT        INTEGER := 0;
    V_MONTHLY_JOB_COUNT      INTEGER := 0;
 
BEGIN
 
    V_LOG := V_LOG || TO_CHAR(:V_START_TIME, 'YYYY-MM-DD HH24:MI:SS') || ' - Procedure started\n';
 
    -- Get configuration count
    SELECT COUNT(*) INTO :V_RECORD_COUNT
    FROM COMMON.TABLE_TO_TABLE_LOAD_CONFIG 
    WHERE RUN_BACKUP_TRIGGER_IND = TRUE;
 
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Found ' || :V_RECORD_COUNT || ' configuration records to process\n';
 
    -- Loop Through Each Configuration Row
    FOR record IN C1 DO
 
        V_TARGET_DB_NAME          := RECORD.TARGET_DB_NAME;
        V_TARGET_SCHEMA_NAME      := RECORD.TARGET_SCHEMA_NAME;
        V_TARGET_TABLE_NAME       := RECORD.TARGET_TABLE_NAME;
        V_SOURCE_DB_NAME          := RECORD.SOURCE_DB_NAME;
        V_SOURCE_SCHEMA_NAME      := RECORD.SOURCE_SCHEMA_NAME;
        V_SOURCE_TABLE_NAME       := RECORD.SOURCE_TABLE_NAME;
        V_LOAD_TYPE               := RECORD.LOAD_TYPE;
        V_FREQUENCY               := RECORD.FREQUENCY
 
        V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Processing: ' || 
                 :V_SOURCE_DB_NAME || '.' || :V_SOURCE_SCHEMA_NAME || '.' || :V_SOURCE_TABLE_NAME || 
                 ' -> ' || :V_TARGET_DB_NAME || '.' || :V_TARGET_SCHEMA_NAME || '.' || :V_TARGET_TABLE_NAME || 
                 ' (' || :V_LOAD_TYPE ||'-' ||V_FREQUENCY || ')\n';
 
        -- Execute the primary load procedure asynchronously
        ASYNC(
            CALL CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP(
                :V_TARGET_DB_NAME,
                :V_TARGET_SCHEMA_NAME,
                :V_TARGET_TABLE_NAME,
                :V_SOURCE_DB_NAME,
                :V_SOURCE_SCHEMA_NAME,
                :V_SOURCE_TABLE_NAME,
                :V_LOAD_TYPE
            )
        );
 
        V_ASYNC_JOB_COUNT := V_ASYNC_JOB_COUNT + 1;
 
        -- Execute additional procedure only for MONTHLY load type
        IF (UPPER(V_FREQUENCY) = 'MONTHLY') THEN
 
            V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Triggering extract table creation for MONTHLY load: ' || :V_SOURCE_TABLE_NAME || '\n';
 
            ASYNC(
                CALL CEDAR_GATE_RESP.CREATE_EXTRACT_TABLE_SP(
                    :V_TARGET_DB_NAME,
                    :V_TARGET_SCHEMA_NAME,
                    :V_SOURCE_TABLE_NAME,
                    :V_SOURCE_DB_NAME,
                    'gxsajja@arkbluecross.com; fajones@arkbluecross.com; swagle@arkbluecross.com'
                )
            );
 
            V_MONTHLY_JOB_COUNT := V_MONTHLY_JOB_COUNT + 1;
        END IF;
 
    END FOR;
 
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Launched ' || :V_ASYNC_JOB_COUNT || ' async load jobs (' || :V_MONTHLY_JOB_COUNT || ' monthly extract jobs)\n';
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Waiting for all async jobs to complete\n';
 
    -- Wait for all async jobs to complete
    AWAIT ALL;
 
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - All async jobs completed\n';
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Duration: ' || DATEDIFF('SECOND', :V_START_TIME, CURRENT_TIMESTAMP()) || ' seconds\n';
    V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - Cedar Gate Table Archival is Complete\n';
 
    RETURN :V_LOG;
 
EXCEPTION
    WHEN OTHER THEN
        V_LOG := V_LOG || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYY-MM-DD HH24:MI:SS') || ' - ERROR: ' || SQLERRM || '\n';
        RETURN :V_LOG;
END;
 
$$;