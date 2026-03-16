

--SFSTRY0113418 - Data Profiling App
CREATE OR REPLACE PROCEDURE {{env}}_DATAMART_DB.STREAMLIT.PROFILE_SCHEMA_COLUMNS_PERSISTENT_SP(
    P_SCHEMA_NAME VARCHAR,
    P_DATABASE_NAME VARCHAR
   //, P_RESULTS_TABLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Set defaults
    var schemaName = P_SCHEMA_NAME;
    var databaseName = P_DATABASE_NAME;
    //var resultsTable = P_RESULTS_TABLE;

    // Handle null database name
    if (!databaseName) {
        var dbStmt = snowflake.createStatement({sqlText: "SELECT CURRENT_DATABASE()"});
        var dbResult = dbStmt.execute();
        dbResult.next();
        databaseName = dbResult.getColumnValue(1);
    }

   /* // Handle null results table name
    if (!resultsTable) {
        resultsTable = 'COLUMN_PROFILE_RESULTS';
    }
    */

    var tablesProcessed = 0;
    var columnsProcessed = 0;
    var runId = '';

    try {
        // Generate UUID
        var uuidStmt = snowflake.createStatement({sqlText: "SELECT UUID_STRING()"});
        var uuidResult = uuidStmt.execute();
        uuidResult.next();
        runId = uuidResult.getColumnValue(1);

   
        // Get all tables in the schema
        var getTablesSQL = 
            "SELECT table_name " +
            "FROM " + databaseName + ".INFORMATION_SCHEMA.TABLES " +
            "WHERE table_schema = '" + schemaName + "' " +
            "  AND table_type = 'BASE TABLE' " +
            "  AND table_catalog = '" + databaseName + "' " +
            "ORDER BY table_name";

        var tableStmt = snowflake.createStatement({sqlText: getTablesSQL});
        var tableResults = tableStmt.execute();

        // Loop through each table
        while (tableResults.next()) {
            var tableName = tableResults.getColumnValue(1);
            tablesProcessed++;

            // Get all columns for this table
            var getColumnsSQL = 
                "SELECT column_name, data_type " +
                "FROM " + databaseName + ".INFORMATION_SCHEMA.COLUMNS " +
                "WHERE table_schema = '" + schemaName + "' " +
                "  AND table_name = '" + tableName + "' " +
                "  AND table_catalog = '" + databaseName + "' " +
                "ORDER BY ordinal_position";

            var columnStmt = snowflake.createStatement({sqlText: getColumnsSQL});
            var columnResults = columnStmt.execute();

            // Loop through each column
            while (columnResults.next()) {
                var columnName = columnResults.getColumnValue(1);
                var dataType = columnResults.getColumnValue(2);
                columnsProcessed++;

                // Build profiling query - escape single quotes in values
                var safeRunId = runId.replace(/'/g, "''");
                var safeTableName = tableName.replace(/'/g, "''");
                var safeColumnName = columnName.replace(/'/g, "''");
                var safeDataType = dataType.replace(/'/g, "''");

                var profileSQL = 
                    'INSERT INTO '+
                    "{{env}}_DATAMART_DB.STREAMLIT.COLUMN_PROFILE_RESULT " +
                    'SELECT ' +
                    "    '" + safeRunId + "' AS PROFILE_RUN_ID, " +
                    "    '" + databaseName + "' AS DATABASE_NAME, " +
                    "    '" + schemaName + "' AS SCHEMA_NAME, " +
                    "    '" + safeTableName + "' AS TABLE_NAME, " +
                    "    '" + safeColumnName + "' AS COLUMN_NAME, " +
                    "    '" + safeDataType + "' AS DATA_TYPE, " +
                    '    COUNT(*) AS TOTAL_ROWS, ' +
                    '    COUNT_IF("' + columnName + '" IS NULL OR LENGTH(TRIM("' + columnName + '")) = 0) AS NULL_COUNT, ' +
                    '    COUNT_IF("' + columnName + '" IS NOT NULL AND LENGTH(TRIM("' + columnName + '")) > 0) AS NOT_NULL_COUNT, ' +
                    '    ROUND((COUNT_IF("' + columnName + '" IS NULL OR  LENGTH(TRIM("' + columnName + '")) = 0) / NULLIF(COUNT(*), 0)) * 100, 2) AS NULL_PERCENTAGE, ' +
                    '    COUNT(DISTINCT "' + columnName + '") AS DISTINCT_COUNT, ' +
                    '    ROUND((COUNT(DISTINCT "' + columnName + '") / NULLIF(COUNT_IF("' + columnName + '" IS NOT NULL AND LENGTH(TRIM("' + columnName + '")) > 0), 0)) * 100, 2) AS DISTINCT_PERCENTAGE, ' +
                    '    TO_VARCHAR(MIN("' + columnName + '")) AS MIN_VALUE, ' +
                    '    TO_VARCHAR(MAX("' + columnName + '")) AS MAX_VALUE, ' +
                    '    CURRENT_TIMESTAMP() AS PROFILE_TIMESTAMP ' +
                    'FROM "' + databaseName + '"."' + schemaName + '"."' + tableName + '"';

                try {
                    snowflake.createStatement({sqlText: profileSQL}).execute();
                } catch (colErr) {
                    // Log column-level errors but continue processing
                    // You could insert into an error log table here

                     return 'Error during profiling: ' + colErr.message + 
               '. Tables processed: ' + tablesProcessed + 
               '. Columns processed: ' + columnsProcessed;
                    
                }
            }
        }

        return 'Profiling completed. Run ID: ' + runId + 
               '. Tables processed: ' + tablesProcessed + 
               '. Columns processed: ' + columnsProcessed;

    } catch (err) {
        return 'Error during profiling: ' + err.message + 
               '. Tables processed: ' + tablesProcessed + 
               '. Columns processed: ' + columnsProcessed;
    }
$$;




