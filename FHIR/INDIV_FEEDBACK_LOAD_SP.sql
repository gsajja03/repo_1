CREATE OR REPLACE PROCEDURE CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP("IN_TARGET_DB_NAME" VARCHAR(16777216), "IN_TARGET_SCHEMA_NAME" VARCHAR(16777216), "IN_TARGET_TABLE_NAME" VARCHAR(16777216), "IN_SOURCE_DB_NAME" VARCHAR(16777216), "IN_SOURCE_SCHEMA_NAME" VARCHAR(16777216), "IN_SOURCE_TABLE_NAME" VARCHAR(16777216), "IN_LOAD_TYPE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try{
//Set attributes for email notification
var v_integration = ''EDM_STRATEGIC_SF_EMAIL_INTEGRATION'';
var target_database_name = IN_TARGET_DB_NAME;
var target_schema_name = IN_TARGET_SCHEMA_NAME;
var target_table_name = IN_TARGET_TABLE_NAME;
var source_database_name = IN_SOURCE_DB_NAME;
var source_schema_name = IN_SOURCE_SCHEMA_NAME;
var source_table_name = IN_SOURCE_TABLE_NAME;
var v_distribution = ''swagle@arkbluecross.com,GXSAJJA@arkbluecross.com,SNOWFLAKE_FILE_LOAD_CONTACTS@arkbluecross.com'';
//var v_distribution = ''swagle@arkbluecross.com,GXSAJJA@arkbluecross.com'';
var sql_cmd_db_nmae = "SELECT CURRENT_DATABASE() ; ";
var db_name_stmt = snowflake.createStatement( {sqlText: sql_cmd_db_nmae} );
var db_name_result_set = db_name_stmt.execute();
db_name_result_set.next()
var v_db_name =    db_name_result_set.getColumnValue(1);   
var v_common_db = "COMMON_DB";
var v_truncate_msg = '''';

//Pull db_prefix variable creation into a Javascript variable 

var prefixStmt_sql_cmd = "SELECT SUBSTR(CURRENT_DATABASE(),1,3) AS PREFIX;";
var prefixStmt = snowflake.createStatement({sqlText: prefixStmt_sql_cmd});
var rs_prefixStmt = prefixStmt.execute();
rs_prefixStmt.next();
var db_prefix = rs_prefixStmt.getColumnValue("PREFIX");


var v_subject = "CEDAR GATE: "+target_database_name +"."+ target_schema_name +"."+ target_table_name+" Table Backup Load.";

if(v_db_name.startsWith("TST") || v_db_name.startsWith("DEV")) 
  { 
    v_common_db =  "TST_COMMON_DB" ;
  }

// Truncate Target table if Load Type is Truncate 

if(IN_LOAD_TYPE == "TRUNCATE_LOAD")
{
var sql_cmd = "TRUNCATE TABLE "+IN_TARGET_DB_NAME+"."+IN_TARGET_SCHEMA_NAME+"."+IN_TARGET_TABLE_NAME+" ; ";
var sql_stmt = snowflake.createStatement( {sqlText: sql_cmd} );
var result_set = sql_stmt.execute();
result_set.next()
v_truncate_msg = "Truncated and "
}

//Copy files from stage to table
var sql_cmd = "INSERT INTO "+IN_TARGET_DB_NAME+"."+IN_TARGET_SCHEMA_NAME+"."+IN_TARGET_TABLE_NAME
//+ "(SELECT *, CURRENT_DATE() FROM PRD_AR_INBOUND_DB."+IN_SOURCE_SCHEMA_NAME+"."+IN_SOURCE_TABLE_NAME+");";
+ "(SELECT *, CURRENT_DATE() FROM "+IN_SOURCE_DB_NAME+"."+IN_SOURCE_SCHEMA_NAME+"."+IN_SOURCE_TABLE_NAME+");";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        
//Record query ID
        var sql_cmd="select last_query_id()";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        var v_query_id = result_set1.getColumnValue(1);

//Record Load Status (create table or insert)
        var sql_cmd=`select * from table(result_scan(''`+v_query_id+`''));`
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        var rec_count = result_set1.getColumnValue(1);

        if(rec_count >= 0) 
        {
        
        var sql_cmd="INSERT INTO "+v_common_db+".AUDIT.FEEDBACK_LOAD_STATUS (TABLE_NAME, TARGET_DB,  TARGET_SCHEMA,  LOAD_DT, QUERY_ID, FILE, STATUS, ROWS_PARSED, ROW_LOADED, ERROR_LIMIT, ERRORS_SEEN, FIRST_ERROR, FIRST_ERROR_LINE, FIRST_ERROR_CHARACTER, FIRST_ERROR_COLUMN_NAME) select ''"+target_table_name+
        "'' AS TABLE_NAME,''"+v_db_name+"'' AS TARGET_DB, ''CEDAR_GATE_RESP'' AS TARGET_SCHEMA, CURRENT_DATE() AS LOAD_DT,''"+v_query_id+"'' AS QUERY_ID , NULL AS FILE, ''LOADED'' AS STATUS, ''"+rec_count+"'' AS ROWS_PARSED, ''"+rec_count+"''  AS ROW_LOADED, NULL AS ERROR_LIMIT, NULL AS ERRORS_SEEN, NULL AS FIRST_ERROR, NULL AS FIRST_ERROR_LINE, NULL AS FIRST_ERROR_CHARACTER, NULL AS FIRST_ERROR_COLUMN_NAME;";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        v_result =  rec_count + " Records have been "+v_truncate_msg+" loaded into "+IN_TARGET_SCHEMA_NAME+"."+IN_TARGET_TABLE_NAME+" from "+IN_SOURCE_DB_NAME+"."+IN_SOURCE_SCHEMA_NAME+"."+IN_SOURCE_TABLE_NAME+", please verify FEEDBACK_LOAD_STATUS with QUERY_ID''"+v_query_id+ "'' for table load details;"
        }
        else 
        {
        v_result = " Back up from"+IN_SOURCE_SCHEMA_NAME+"."+IN_SOURCE_TABLE_NAME+" to "+IN_TARGET_SCHEMA_NAME+"."+IN_TARGET_TABLE_NAME+", Failed, please verify load status using query ID: ''"+v_query_id+ "'';"
        }

//Update RUN_BACKUP_TRIGGER_IND To FALSE Where RUN_BACKUP_TRIGGER_IND = TRUE in the COMMON.TABLE_TO_TABLE_LOAD_CONFIG Table

        var update_run_backup_trigger_ind_sql_cmd = "UPDATE "+db_prefix+"_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG SET RUN_BACKUP_TRIGGER_IND = FALSE WHERE RUN_BACKUP_TRIGGER_IND = TRUE AND TARGET_TABLE_NAME = ''"+IN_TARGET_TABLE_NAME+"'';";
        var update_run_backup_trigger_ind_statement = snowflake.createStatement({sqlText: update_run_backup_trigger_ind_sql_cmd});
        var update_run_backup_trigger_ind_result_set = update_run_backup_trigger_ind_statement.execute();
        update_run_backup_trigger_ind_result_set.next();




//Update ARCHIVE_STATUS To Y On CONTROL_TOTAL Table 

        var update_archive_status_sql_cmd = "UPDATE "+db_prefix+"_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL SET ARCHIVE_STATUS = ''Y'' WHERE ARCHIVE_STATUS = ''N'' AND (SELECT COUNT(*) FROM "+db_prefix+"_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG WHERE  RUN_BACKUP_TRIGGER_IND = TRUE AND SOURCE_TABLE_NAME = ''"+IN_SOURCE_TABLE_NAME+"'' AND TARGET_TABLE_NAME NOT  LIKE ''%INCREMENTAL%'' ) = 0;";
        var update_archive_status_statement = snowflake.createStatement({sqlText: update_archive_status_sql_cmd});
        var update_archive_status_result_set = update_archive_status_statement.execute();
        update_archive_status_result_set.next();


const results = snowflake.execute({
            sqlText: `call SYSTEM$SEND_EMAIL(?,?,?,?)`,
            binds: [v_integration, v_distribution, v_subject, v_result ],
            });            
        results.next();
        return v_result
         }
        catch (err)
       {
          result =  "Failed: Code: " + err.code + "\\n  State: " + err.state;
          result += "\\n  Message: " + err.message;
          result += "\\nStack Trace:\\n" + err.stackTraceTxt;
          result +=sql_cmd
          return result
       }
        ';

/* 
CALL DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP(
        'DEV_RAW_INBOUND_DB',
        'CEDAR_GATE_RESP',
        'MEDICAL_CLAIMS',
        'DEV_AR_INBOUND_DB',
        'CEDAR_GATE',
        'MEDICAL_CLAIMS',
        'APPEND' );


SELECT * FROM PRD_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG WHERE  RUN_BACKUP_TRIGGER_IND = TRUE 
AND SOURCE_TABLE_NAME = 'MEDICAL_CLAIMS' AND TARGET_TABLE_NAME NOT  LIKE '%INCREMENTAL%' AND SOURCE_TABLE_NAME <> TARGET_TABLE_NAME;
*/

/*
AI - Generated *******************
"Certainly! Below is a conversion of the provided JavaScript-based stored procedure into an equivalent SQL-based stored procedure using Snowflake's procedural language (PL/pgSQL). This version maintains the same functionality as the original JavaScript code.

```sql
CREATE OR REPLACE PROCEDURE CEDAR_GATE_RESP.CEDAR_GATE_INDIV_FEEDBACK_LOAD_SP(
    IN_TARGET_DB_NAME VARCHAR(16777216),
    IN_TARGET_SCHEMA_NAME VARCHAR(16777216),
    IN_TARGET_TABLE_NAME VARCHAR(16777216),
    IN_SOURCE_DB_NAME VARCHAR(16777216),
    IN_SOURCE_SCHEMA_NAME VARCHAR(16777216),
    IN_SOURCE_TABLE_NAME VARCHAR(16777216),
    IN_LOAD_TYPE VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS $$
DECLARE
    v_integration VARCHAR := 'EDM_STRATEGIC_SF_EMAIL_INTEGRATION';
    target_database_name VARCHAR := IN_TARGET_DB_NAME;
    target_schema_name VARCHAR := IN_TARGET_SCHEMA_NAME;
    target_table_name VARCHAR := IN_TARGET_TABLE_NAME;
    source_database_name VARCHAR := IN_SOURCE_DB_NAME;
    source_schema_name VARCHAR := IN_SOURCE_SCHEMA_NAME;
    source_table_name VARCHAR := IN_SOURCE_TABLE_NAME;
    v_distribution VARCHAR := 'swagle@arkbluecross.com,GXSAJJA@arkbluecross.com,SNOWFLAKE_FILE_LOAD_CONTACTS@arkbluecross.com';
    sql_cmd_db_nmae VARCHAR := 'SELECT CURRENT_DATABASE() ; ';
    db_name_stmt RESULTSET;
    db_name_result_set RESULTSET;
    v_db_name VARCHAR;
    v_common_db VARCHAR := 'COMMON_DB';
    v_truncate_msg VARCHAR := '';
    prefixStmt_sql_cmd VARCHAR := 'SELECT SUBSTR(CURRENT_DATABASE(),1,3) AS PREFIX;';
    prefixStmt RESULTSET;
    rs_prefixStmt RESULTSET;
    db_prefix VARCHAR;
    v_subject VARCHAR;
    sql_cmd VARCHAR;
    sql_stmt STATEMENT;
    result_set RESULTSET;
    rec_count INT;
    v_query_id VARCHAR;
    update_run_backup_trigger_ind_sql_cmd VARCHAR;
    update_run_backup_trigger_ind_statement STATEMENT;
    update_run_backup_trigger_ind_result_set RESULTSET;
    update_archive_status_sql_cmd VARCHAR;
    update_archive_status_statement STATEMENT;
    update_archive_status_result_set RESULTSET;
    v_result VARCHAR;
BEGIN
    -- Set attributes for email notification
    IF IN_LOAD_TYPE = 'TRUNCATE_LOAD' THEN
        v_truncate_msg := 'Truncated and ';
    END IF;

    -- Pull db_prefix variable creation into a SQL variable
    prefixStmt := snowflake.createStatement({sqlText: prefixStmt_sql_cmd});
    rs_prefixStmt := prefixStmt.execute();
    rs_prefixStmt.next();
    db_prefix := rs_prefixStmt.getColumnValue(""PREFIX"");

    v_subject := 'CEDAR GATE: ' || target_database_name || '.' || target_schema_name || '.' || target_table_name || ' Table Backup Load.';

    IF v_db_name LIKE 'TST%' OR v_db_name LIKE 'DEV%' THEN
        v_common_db := 'TST_COMMON_DB';
    END IF;

    -- Truncate Target table if Load Type is TRUNCATE
    IF IN_LOAD_TYPE = 'TRUNCATE_LOAD' THEN
        sql_cmd := 'TRUNCATE TABLE ' || IN_TARGET_DB_NAME || '.' || IN_TARGET_SCHEMA_NAME || '.' || IN_TARGET_TABLE_NAME || ';';
        sql_stmt := snowflake.createStatement({sqlText: sql_cmd});
        result_set := sql_stmt.execute();
    END IF;

    -- Copy files from stage to table
    sql_cmd := 'INSERT INTO ' || IN_TARGET_DB_NAME || '.' || IN_TARGET_SCHEMA_NAME || '.' || IN_TARGET_TABLE_NAME
              || ' (SELECT *, CURRENT_DATE() FROM ' || IN_SOURCE_DB_NAME || '.' || IN_SOURCE_SCHEMA_NAME || '.' || IN_SOURCE_TABLE_NAME || ');';
    sql_stmt := snowflake.createStatement({sqlText: sql_cmd});
    result_set := sql_stmt.execute();

    -- Record query ID
    sql_cmd := 'select last_query_id();';
    sql_stmt := snowflake.createStatement({sqlText: sql_cmd});
    result_set := sql_stmt.execute();
    result_set.next();
    v_query_id := result_set.getColumnValue(1);

    -- Record Load Status (create table or insert)
    sql_cmd := 'select * from table(result_scan(''' || v_query_id || '''));';
    sql_stmt := snowflake.createStatement({sqlText: sql_cmd});
    result_set := sql_stmt.execute();
    result_set.next();
    rec_count := result_set.getColumnValue(1);

    IF rec_count >= 0 THEN
        sql_cmd := 'INSERT INTO ' || v_common_db || '.AUDIT.FEEDBACK_LOAD_STATUS (TABLE_NAME, TARGET_DB, TARGET_SCHEMA, LOAD_DT, QUERY_ID, FILE, STATUS, ROWS_PARSED, ROW_LOADED, ERROR_LIMIT, ERRORS_SEEN, FIRST_ERROR, FIRST_ERROR_LINE, FIRST_ERROR_CHARACTER, FIRST_ERROR_COLUMN_NAME) '
                  || 'select ''' || target_table_name || ''', ''' || v_db_name || ''', ''CEDAR_GATE_RESP'', CURRENT_DATE(), ''' || v_query_id || ''', NULL, ''LOADED'', ''' || rec_count || ''', ''' || rec_count || ''', NULL, NULL, NULL, NULL, NULL, NULL;';
        sql_stmt := snowflake.createStatement({sqlText: sql_cmd});
        result_set := sql_stmt.execute();
        v_result := rec_count || ' Records have been ' || v_truncate_msg || ' loaded into ' || IN_TARGET_SCHEMA_NAME || '.' || IN_TARGET_TABLE_NAME || ' from ' || IN_SOURCE_DB_NAME || '.' || IN_SOURCE_SCHEMA_NAME || '.' || IN_SOURCE_TABLE_NAME || ', please verify FEEDBACK_LOAD_STATUS with QUERY_ID ''' || v_query_id || '''' || ' for table load details.';
    ELSE
        v_result := 'Back up from ' || IN_SOURCE_SCHEMA_NAME || '.' || IN_SOURCE_TABLE_NAME || ' to ' || IN_TARGET_SCHEMA_NAME || '.' || IN_TARGET_TABLE_NAME || ', Failed, please verify load status using query ID: ''' || v_query_id || '''.';
    END IF;

    -- Update RUN_BACKUP_TRIGGER_IND To FALSE Where RUN_BACKUP_TRIGGER_IND = TRUE in the COMMON.TABLE_TO_TABLE_LOAD_CONFIG Table
    update_run_backup_trigger_ind_sql_cmd := 'UPDATE ' || db_prefix || '_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG SET RUN_BACKUP_TRIGGER_IND = FALSE WHERE RUN_BACKUP_TRIGGER_IND = TRUE AND TARGET_TABLE_NAME = ''' || IN_TARGET_TABLE_NAME || ''';';
    update_run_backup_trigger_ind_statement := snowflake.createStatement({sqlText: update_run_backup_trigger_ind_sql_cmd});
    update_run_backup_trigger_ind_result_set := update_run_backup_trigger_ind_statement.execute();

    -- Update ARCHIVE_STATUS To Y On CONTROL_TOTAL Table
    update_archive_status_sql_cmd := 'UPDATE ' || db_prefix || '_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL SET ARCHIVE_STATUS = ''Y'' WHERE ARCHIVE_STATUS = ''N'' AND (SELECT COUNT(*) FROM ' || db_prefix || '_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG WHERE RUN_BACKUP_TRIGGER_IND = TRUE AND SOURCE_TABLE_NAME = ''' || IN_SOURCE_TABLE_NAME || ''' AND TARGET_TABLE_NAME NOT LIKE ''%INCREMENTAL%'') = 0;';
    update_archive_status_statement := snowflake.createStatement({sqlText: update_archive_status_sql_cmd});
    update_archive_status_result_set := update_archive_status_statement.execute();

    -- Email Notification
    EXECUTE IMMEDIATE 'call SYSTEM$SEND_EMAIL(?, ?, ?, ?)' USING v_integration, v_distribution, v_subject, v_result;

    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        v_result := 'Failed: Code: ' || SQLCODE || ' State: ' || SQLSTATE || ' Message: ' || SQLERRM || ' Stack Trace: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE();
        EXECUTE IMMEDIATE 'call SYSTEM$SEND_EMAIL(?, ?, ?, ?)' USING v_integration, v_distribution, v_subject, v_result;
        RETURN v_result;
END;
$$;
```

### Explanation:

1. **Variable Declarations**: Variables are declared to hold various values such as database names, SQL commands, and results.
2. **Email Notification Setup**: Sets up email attributes for integration, distribution list, and subject.
3. **Database Prefix Retrieval**: Retrieves the database prefix using a SQL query.
4. **Truncate Target Table (if applicable)**: Truncates the target table if the load type is `TRUNCATE_LOAD`.
5. **Data Loading**: Inserts data from the source table into the target table, appending the current date to each row.
6. **Query ID and Load Status Recording**: Records the query ID of the insert operation and retrieves the number of rows loaded.
7. **Update Configuration Tables**: Updates `RUN_BACKUP_TRIGGER_IND` in the configuration table to `FALSE` and updates `ARCHIVE_STATUS` in the control total table to `'Y'`.
8. **Email Notification**: Sends an email notification with the load status and details.
9. **Error Handling**: Catches and logs any errors that occur during execution.

This SQL-based stored procedure should function similarly to the original JavaScript version, providing a robust solution for loading data from a source table to a target table in Snowflake with additional features such as truncation, logging, and email notifications."




"Certainly! Below is a conversion of the provided JavaScript-based stored procedure into an equivalent Python script using Snowflake's Python connector (snowflake-connector-python). This version maintains the same functionality as the original JavaScript code.

### Prerequisites

Ensure you have the `snowflake-connector-python` package installed. You can install it using pip:

```sh
pip install snowflake-connector-python
```

### Python Script

```python
import snowflake.connector
from snowflake.connector import Error

def cedar_gate_indiv_feedback_load_sp(
    in_target_db_name,
    in_target_schema_name,
    in_target_table_name,
    in_source_db_name,
    in_source_schema_name,
    in_source_table_name,
    in_load_type
):
    try:
        # Establish a connection to Snowflake
        conn = snowflake.connector.connect(
            user='your_username',
            password='your_password',
            account='your_account',
            warehouse='your_warehouse',
            database='your_database',
            schema='your_schema'
        )
        cursor = conn.cursor()

        # Set attributes for email notification
        v_integration = 'EDM_STRATEGIC_SF_EMAIL_INTEGRATION'
        target_database_name = in_target_db_name
        target_schema_name = in_target_schema_name
        target_table_name = in_target_table_name
        source_database_name = in_source_db_name
        source_schema_name = in_source_schema_name
        source_table_name = in_source_table_name
        v_distribution = 'swagle@arkbluecross.com,GXSAJJA@arkbluecross.com,SNOWFLAKE_FILE_LOAD_CONTACTS@arkbluecross.com'
        v_subject = f""CEDAR GATE: {target_database_name}.{target_schema_name}.{target_table_name} Table Backup Load.""
        v_truncate_msg = ''

        # Pull db_prefix variable creation into a Python variable
        prefix_stmt_sql_cmd = ""SELECT SUBSTR(CURRENT_DATABASE(), 1, 3) AS PREFIX;""
        cursor.execute(prefix_stmt_sql_cmd)
        rs_prefixStmt = cursor.fetchone()
        db_prefix = rs_prefixStmt[0]

        if in_load_type == 'TRUNCATE_LOAD':
            v_truncate_msg = 'Truncated and '

        # Truncate Target table if Load Type is TRUNCATE
        if in_load_type == 'TRUNCATE_LOAD':
            sql_cmd = f""TRUNCATE TABLE {in_target_db_name}.{in_target_schema_name}.{in_target_table_name};""
            cursor.execute(sql_cmd)

        # Copy files from stage to table
        sql_cmd = (f""INSERT INTO {in_target_db_name}.{in_target_schema_name}.{in_target_table_name} ""
                   f""(SELECT *, CURRENT_DATE() FROM {source_database_name}.{source_schema_name}.{source_table_name});"")
        cursor.execute(sql_cmd)

        # Record query ID
        sql_cmd = ""select last_query_id();""
        cursor.execute(sql_cmd)
        v_query_id = cursor.fetchone()[0]

        # Record Load Status (create table or insert)
        sql_cmd = f""select * from table(result_scan('{v_query_id}'));""
        cursor.execute(sql_cmd)
        rec_count = cursor.fetchone()[0]

        if rec_count >= 0:
            sql_cmd = (f""INSERT INTO {db_prefix}_RAW_INBOUND_DB.AUDIT.FEEDBACK_LOAD_STATUS ""
                       f""(TABLE_NAME, TARGET_DB, TARGET_SCHEMA, LOAD_DT, QUERY_ID, FILE, STATUS, ROWS_PARSED, ROW_LOADED, ERROR_LIMIT, ERRORS_SEEN, FIRST_ERROR, FIRST_ERROR_LINE, FIRST_ERROR_CHARACTER, FIRST_ERROR_COLUMN_NAME) ""
                       f""select '{target_table_name}', CURRENT_DATABASE(), 'CEDAR_GATE_RESP', CURRENT_DATE(), '{v_query_id}', NULL, 'LOADED', {rec_count}, {rec_count}, NULL, NULL, NULL, NULL, NULL;"")
            cursor.execute(sql_cmd)
            v_result = (f""{rec_count} Records have been {v_truncate_msg} loaded into {in_target_schema_name}.{in_target_table_name} ""
                       f""from {source_database_name}.{source_schema_name}.{source_table_name}, please verify FEEDBACK_LOAD_STATUS with QUERY_ID '{v_query_id}' for table load details."")
        else:
            v_result = (f""Back up from {source_schema_name}.{source_table_name} to {in_target_schema_name}.{in_target_table_name}, ""
                       f""Failed, please verify load status using query ID: '{v_query_id}'."")

        # Update RUN_BACKUP_TRIGGER_IND To FALSE Where RUN_BACKUP_TRIGGER_IND = TRUE in the COMMON.TABLE_TO_TABLE_LOAD_CONFIG Table
        update_run_backup_trigger_ind_sql_cmd = (f""UPDATE {db_prefix}_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG ""
                                                f""SET RUN_BACKUP_TRIGGER_IND = FALSE WHERE RUN_BACKUP_TRIGGER_IND = TRUE AND TARGET_TABLE_NAME = '{in_target_table_name}';"")
        cursor.execute(update_run_backup_trigger_ind_sql_cmd)

        # Update ARCHIVE_STATUS To Y On CONTROL_TOTAL Table
        update_archive_status_sql_cmd = (f""UPDATE {db_prefix}_RAW_INBOUND_DB.CEDAR_GATE_RESP.CONTROL_TOTAL ""
                                        f""SET ARCHIVE_STATUS = 'Y' WHERE ARCHIVE_STATUS = 'N' AND ""
                                        f""(SELECT COUNT(*) FROM {db_prefix}_RAW_INBOUND_DB.COMMON.TABLE_TO_TABLE_LOAD_CONFIG ""
                                        f""WHERE RUN_BACKUP_TRIGGER_IND = TRUE AND SOURCE_TABLE_NAME = '{source_table_name}' AND TARGET_TABLE_NAME NOT LIKE '%INCREMENTAL%') = 0;"")
        cursor.execute(update_archive_status_sql_cmd)

        # Email Notification
        send_email(v_integration, v_distribution, v_subject, v_result)

        return v_result

    except Error as e:
        v_result = (f""Failed: Code: {e.errno} State: {e.sqlstate} Message: {e.msg} ""
                   f""Stack Trace: {e.traceback}"")
        send_email(v_integration, v_distribution, v_subject, v_result)
        return v_result

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def send_email(integration, distribution, subject, message):
    # Implement your email sending logic here
    print(f""Sending email to {distribution} with subject: {subject}"")
    print(f""Message: {message}"")

# Example usage
if __name__ == ""__main__"":
    result = cedar_gate_indiv_feedback_load_sp(
        in_target_db_name='your_target_db',
        in_target_schema_name='your_target_schema',
        in_target_table_name='your_target_table',
        in_source_db_name='your_source_db',
        in_source_schema_name='your_source_schema',
        in_source_table_name='your_source_table',
        in_load_type='TRUNCATE_LOAD'
    )
    print(result)
```

### Explanation:

1. **Connection Setup**: Establishes a connection to Snowflake using the `snowflake.connector` library.
2. **Variable Declarations**: Variables are declared to hold various values such as database names, SQL commands, and results.
3. **Email Notification Setup**: Sets up email attributes for integration, distribution list, and subject.
4. **Database Prefix Retrieval**: Retrieves the database prefix using a SQL query.
5. **Truncate Target Table (if applicable)**: Truncates the target table if the load type is `TRUNCATE_LOAD`.
6. **Data Loading**: Inserts data from the source table into the target table, appending the current date to each row.
7. **Query ID and Load Status Recording**: Records the query ID of the insert operation and retrieves the number of rows loaded.
8. **Update Configuration Tables**: Updates `RUN_BACKUP_TRIGGER_IND` in the configuration table to `FALSE` and updates `ARCHIVE_STATUS` in the control total table to `'Y'`.
9. **Email Notification**: Sends an email notification with the load status and details using a placeholder function `send_email`. You can replace this with your actual email sending logic.
10. **Error Handling**: Catches and logs any errors that occur during execution.

This Python script should function similarly to the original JavaScript version, providing a robust solution for loading data from a source table to a target table in Snowflake with additional features such as truncation, logging, and email notifications."





 */