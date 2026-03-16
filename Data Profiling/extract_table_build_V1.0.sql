CREATE OR REPLACE PROCEDURE CEDAR_GATE_RESP.CREATE_EXTRACT_TABLE_SP(
    P_TARGET_DATABASE VARCHAR,
    P_TARGET_SCHEMA VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_AR_INBOUND_DB VARCHAR,
    P_EMAIL_LIST VARCHAR
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var results = [];
    var errorLog = [];

    try {
        // Input parameters
        var targetDatabase = P_TARGET_DATABASE;
        var targetSchema = P_TARGET_SCHEMA;
        var tableName = P_TABLE_NAME;
        var arInboundDb = P_AR_INBOUND_DB;
        var recipientEmail = P_EMAIL_LIST;

        // Validate inputs
        if (!targetDatabase || !targetSchema || !tableName || !arInboundDb) {
            return "Error: All parameters (P_TARGET_DATABASE, P_TARGET_SCHEMA, P_TABLE_NAME, P_AR_INBOUND_DB) are required.";
        }

        results.push(`Processing table: ${targetDatabase}.${targetSchema}.${tableName}_EXTRACT`);
        results.push(`AR Inbound Database: ${arInboundDb}`);
        results.push(`Start Time: ${new Date().toISOString()}`);

        // Get column mappings from the driver query for the specific table
        var driverQuery = `
            SELECT 
                B.TABLE_NAME,
                B.COLUMN_NAME,
                B.ORDINAL_POSITION,
                NVL(A.ABCBS_FIELD_DESC, B.COLUMN_NAME) AS COLUMN_ALIAS
            FROM (
                SELECT 
                    TABLE_NAME,
                    COLUMN_NAME,
                    ORDINAL_POSITION 
                FROM `+arInboundDb+`.INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = 'CEDAR_GATE' 
                    AND TABLE_NAME IN (
                        SELECT DISTINCT TABLE_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA LIKE '%CEDAR_GATE%'
                    )
            ) B
            LEFT JOIN `+arInboundDb+`.CEDAR_GATE.EXTRACT_FIELD_NAMES A
                ON A.TABLE_NAME = B.TABLE_NAME 
                AND A.EXTRACT_FIELD_NAME = B.COLUMN_NAME
            WHERE B.TABLE_NAME = ?
            ORDER BY B.ORDINAL_POSITION
        `;

        var driverStmt = snowflake.createStatement({
            sqlText: driverQuery,
            binds: [tableName]
        });
        var driverResult = driverStmt.execute();

        // Collect column mappings
        var columns = [];
        var rowCount = 0;

        while (driverResult.next()) {
            var columnName = driverResult.getColumnValue('COLUMN_NAME');
            var columnAlias = driverResult.getColumnValue('COLUMN_ALIAS');
            var ordinalPosition = driverResult.getColumnValue('ORDINAL_POSITION');

            columns.push({
                originalName: columnName,
                aliasName: columnAlias,
                position: ordinalPosition
            });
            rowCount++;
        }

        if (rowCount === 0) {
            var errorMsg = `Error: No columns found for table ${tableName}. Please verify the table exists and is configured in the mapping.`;

            // Send error email
            sendEmail(recipientEmail, tableName, 'FAILED', [errorMsg], [], arInboundDb);

            return errorMsg;
        }

        results.push(`Found ${rowCount} columns to process`);

        // Define source and target table names
        var extractTableName = tableName + '_EXTRACT';
        var sourceTable = targetDatabase + '.' + targetSchema + '.' + tableName;
        var targetTable = targetDatabase + '.' + targetSchema + '.' + extractTableName;
        var dataSourceTable = arInboundDb + '.CEDAR_GATE.' + tableName;

        // Step 1: Create table like source
        results.push(`\n=== Step 1: Creating Table Structure ===`);
        results.push(`Creating table: ${targetTable}`);
        results.push(`Based on structure from: ${sourceTable}`);

        var createTableSQL = `
            CREATE OR REPLACE TABLE ${targetTable} 
            LIKE ${sourceTable}
        `;

        var createStmt = snowflake.createStatement({sqlText: createTableSQL});
        createStmt.execute();

        results.push(`✓ Successfully created table: ${extractTableName}`);

        // Step 2: Rename columns based on aliases
        results.push(`\n=== Step 2: Renaming Columns ===`);
        var renameCount = 0;
        var skippedCount = 0;
        var columnMapping = {}; // Store original to new name mapping

        for (var i = 0; i < columns.length; i++) {
            var col = columns[i];

            // Only rename if alias is different from original name
            if (col.originalName !== col.aliasName) {

                // Sanitize column alias to ensure it's a valid identifier

                var sanitizedAlias = col.aliasName;
                
                /*var sanitizedAlias = col.aliasName.replace(/[^a-zA-Z0-9_]/g, '_');

                // Ensure alias doesn't start with a number
                if (/^[0-9]/.test(sanitizedAlias)) {
                    sanitizedAlias = 'COL_' + sanitizedAlias;
                }*/

                var renameSQL = `
                    ALTER TABLE ${targetTable} 
                    RENAME COLUMN "${col.originalName}" TO "${sanitizedAlias}"
                `;

                try {
                    var renameStmt = snowflake.createStatement({sqlText: renameSQL});
                    renameStmt.execute();
                    renameCount++;
                    columnMapping[col.originalName] = sanitizedAlias;
                    results.push(`  ✓ Renamed: ${col.originalName} → ${sanitizedAlias}`);
                } catch (renameErr) {
                    var errMsg = `Error renaming column ${col.originalName} to ${sanitizedAlias}: ${renameErr.message}`;
                    errorLog.push(`  ✗ ${errMsg}`);
                    // Keep original name in mapping if rename fails
                    columnMapping[col.originalName] = col.originalName;
                }
            } else {
                skippedCount++;
                columnMapping[col.originalName] = col.originalName;
            }
        }

        results.push(`Renamed: ${renameCount} columns`);
        results.push(`Skipped: ${skippedCount} columns (no change needed)`);

        // Step 3: Copy data from AR_INBOUND_DB.CEDAR_GATE source table
        results.push(`\n=== Step 3: Copying Data ===`);
        results.push(`Source: ${dataSourceTable}`);
        results.push(`Target: ${targetTable}`);

        try {
            // First, check if source table exists
            var checkSourceSQL = `
                SELECT COUNT(*) AS TABLE_EXISTS
                FROM ${arInboundDb}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'CEDAR_GATE'
                AND TABLE_NAME = ?
            `;

            var checkStmt = snowflake.createStatement({
                sqlText: checkSourceSQL,
                binds: [tableName]
            });
            var checkResult = checkStmt.execute();
            checkResult.next();
            var tableExists = checkResult.getColumnValue('TABLE_EXISTS');

            if (tableExists === 0) {
                var warningMsg = `Warning: Source table ${dataSourceTable} does not exist. Skipping data copy.`;
                results.push(`⚠ ${warningMsg}`);
                errorLog.push(warningMsg);
            } else {
                // Get row count from source before copy
                var countSourceSQL = `SELECT COUNT(*) AS ROW_COUNT FROM ${dataSourceTable}`;
                var countStmt = snowflake.createStatement({sqlText: countSourceSQL});
                var countResult = countStmt.execute();
                countResult.next();
                var sourceRowCount = countResult.getColumnValue('ROW_COUNT');

                results.push(`Source table row count: ${sourceRowCount}`);

                if (sourceRowCount === 0) {
                    results.push(`⚠ Source table is empty. No data to copy.`);
                } else {
                    // Build column list for INSERT SELECT
                    // Use original column names from source, which map to new names in target
                    var sourceColumns = [];
                    var targetColumns = [];

                    for (var i = 0; i < columns.length; i++) {
                        var originalName = columns[i].originalName;
                        sourceColumns.push('"' + originalName + '"');
                        targetColumns.push('"' + columns[i].aliasName + '"');
                    }

                    var sourceColumnList = sourceColumns.join(', ');
                    var targetColumnList = targetColumns.join(', ');

                    // Copy data using INSERT INTO SELECT
                    var copyDataSQL = `
                        INSERT INTO ${targetTable} (${targetColumnList})
                        SELECT ${sourceColumnList}
                        FROM ${dataSourceTable}
                    `;

                    results.push(`Executing data copy...`);
                    var copyStartTime = new Date();

                    var copyStmt = snowflake.createStatement({sqlText: copyDataSQL});
                    var copyResult = copyStmt.execute();

                    var copyEndTime = new Date();
                    var copyDuration = (copyEndTime - copyStartTime) / 1000; // seconds

                    // Get row count from target after copy
                    var countTargetSQL = `SELECT COUNT(*) AS ROW_COUNT FROM ${targetTable}`;
                    var countTargetStmt = snowflake.createStatement({sqlText: countTargetSQL});
                    var countTargetResult = countTargetStmt.execute();
                    countTargetResult.next();
                    var targetRowCount = countTargetResult.getColumnValue('ROW_COUNT');

                    results.push(`✓ Data copy completed successfully!`);
                    results.push(`  - Rows copied: ${targetRowCount}`);
                    results.push(`  - Duration: ${copyDuration.toFixed(2)} seconds`);

                    // Calculate throughput
                    if (copyDuration > 0) {
                        var throughput = (targetRowCount / copyDuration).toFixed(0);
                        results.push(`  - Throughput: ${throughput} rows/second`);
                    }

                    // Verify row counts match
                    if (sourceRowCount !== targetRowCount) {
                        var mismatchMsg = `Warning: Row count mismatch! Source: ${sourceRowCount}, Target: ${targetRowCount}`;
                        results.push(`⚠ ${mismatchMsg}`);
                        errorLog.push(mismatchMsg);
                    } else {
                        results.push(`✓ Row count verification passed`);
                    }
                }
            }

        } catch (copyErr) {
            var copyErrMsg = `Error copying data: ${copyErr.message}`;
            results.push(`✗ ${copyErrMsg}`);
            errorLog.push(copyErrMsg);
        }

        // Final Summary
        results.push(`\n=== Final Summary ===`);
        results.push(`End Time: ${new Date().toISOString()}`);
        results.push(`Total columns: ${columns.length}`);
        results.push(`Columns renamed: ${renameCount}`);
        results.push(`Columns skipped: ${skippedCount}`);
        results.push(`Errors encountered: ${errorLog.length}`);

        // Determine status
        var status = errorLog.length === 0 ? 'SUCCESS' : 'COMPLETED_WITH_ERRORS';

        // Send email notification
        sendEmail(recipientEmail, tableName, status, results, errorLog, arInboundDb);

        // Prepare return message
        var returnMsg = results.join("\n");

        if (errorLog.length > 0) {
            returnMsg += "\n\n--- Errors ---\n" + errorLog.join("\n");
        }

        returnMsg += "\n\n✓ Process completed!";
        returnMsg += `\nEmail notification sent to: ${recipientEmail}`;

        return returnMsg;

    } catch (err) {
        var fatalError = "Fatal error: " + err.message + "\n\nStack trace:\n" + err.stack;

        // Send error email
        try {
            sendEmail(recipientEmail, P_TABLE_NAME || 'UNKNOWN', 'FAILED', [fatalError], [], P_AR_INBOUND_DB || 'UNKNOWN');
        } catch (emailErr) {
            // If email fails, just log it
            fatalError += "\n\nFailed to send email notification: " + emailErr.message;
        }

        return fatalError;
    }

    // Function to send email with proper HTML rendering
    function sendEmail(recipient, tableName, status, resultLog, errorLog, arInboundDb) {
        try {
            // Determine subject based on status
            var subject = '';
            var statusIcon = '';
            if (status === 'SUCCESS') {
                subject = 'SUCCESS: Extract Table Created - ' + tableName + '_EXTRACT';
                statusIcon = '✓';
            } else if (status === 'COMPLETED_WITH_ERRORS') {
                subject = 'WARNING: Extract Table Created with Errors - ' + tableName + '_EXTRACT';
                statusIcon = '⚠';
            } else {
                subject = 'FAILED: Extract Table Creation Failed - ' + tableName;
                statusIcon = '✗';
            }

            // Build HTML email body with proper escaping
            var htmlBody = '<!DOCTYPE html>\n';
            htmlBody += '<html>\n';
            htmlBody += '<head>\n';
            htmlBody += '<meta charset="UTF-8">\n';
            htmlBody += '<style>\n';
            htmlBody += '  body { font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; }\n';
            htmlBody += '  .container { max-width: 800px; margin: 0 auto; padding: 20px; }\n';
            htmlBody += '  .header { background-color: ' + (status === 'SUCCESS' ? '#4CAF50' : status === 'FAILED' ? '#f44336' : '#ff9800') + '; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }\n';
            htmlBody += '  .header h2 { margin: 0; }\n';
            htmlBody += '  .section { margin: 20px 0; padding: 15px; background-color: #f5f5f5; border-radius: 5px; border-left: 4px solid #4CAF50; }\n';
            htmlBody += '  .error-section { border-left-color: #f44336; }\n';
            htmlBody += '  .success { color: #4CAF50; font-weight: bold; }\n';
            htmlBody += '  .error { color: #f44336; font-weight: bold; }\n';
            htmlBody += '  .warning { color: #ff9800; font-weight: bold; }\n';
            htmlBody += '  .log-content { background-color: #ffffff; padding: 15px; border: 1px solid #ddd; border-radius: 3px; overflow-x: auto; font-family: "Courier New", monospace; font-size: 12px; white-space: pre-wrap; word-wrap: break-word; }\n';
            htmlBody += '  table { border-collapse: collapse; width: 100%; margin: 10px 0; background-color: white; }\n';
            htmlBody += '  th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }\n';
            htmlBody += '  th { background-color: #4CAF50; color: white; font-weight: bold; }\n';
            htmlBody += '  tr:nth-child(even) { background-color: #f9f9f9; }\n';
            htmlBody += '  .footer { margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }\n';
            htmlBody += '</style>\n';
            htmlBody += '</head>\n';
            htmlBody += '<body>\n';
            htmlBody += '<div class="container">\n';

            // Header
            htmlBody += '  <div class="header">\n';
            htmlBody += '    <h2>' + statusIcon + ' ' + subject + '</h2>\n';
            htmlBody += '  </div>\n';

            // Process Details Section
            htmlBody += '  <div class="section">\n';
            htmlBody += '    <h3>Process Details</h3>\n';
            htmlBody += '    <table>\n';
            htmlBody += '      <tr><th>Property</th><th>Value</th></tr>\n';
            htmlBody += '      <tr><td><strong>Table Name</strong></td><td>' + tableName + '</td></tr>\n';
            htmlBody += '      <tr><td><strong>Extract Table</strong></td><td>' + tableName + '_EXTRACT</td></tr>\n';
            htmlBody += '      <tr><td><strong>Target Location</strong></td><td>'+ targetDatabase + '.' + targetSchema + '</td></tr>\n';
            htmlBody += '      <tr><td><strong>Data Source Database</strong></td><td>' + arInboundDb + '</td></tr>\n';
            htmlBody += '      <tr><td><strong>Data Source Table</strong></td><td>' + arInboundDb + '.CEDAR_GATE.' + tableName + '</td></tr>\n';
            htmlBody += '      <tr><td><strong>Status</strong></td><td><span class="' + (status === 'SUCCESS' ? 'success' : status === 'FAILED' ? 'error' : 'warning') + '">' + status + '</span></td></tr>\n';
            htmlBody += '    </table>\n';
            htmlBody += '  </div>\n';

            // Process Log Section
            if (resultLog.length > 0) {
                htmlBody += '  <div class="section">\n';
                htmlBody += '    <h3>Process Log</h3>\n';
                htmlBody += '    <div class="log-content">' + escapeHtml(resultLog.join('\n')) + '</div>\n';
                htmlBody += '  </div>\n';
            }

            // Errors Section
            if (errorLog.length > 0) {
                htmlBody += '  <div class="section error-section">\n';
                htmlBody += '    <h3 class="error">Errors Encountered (' + errorLog.length + ')</h3>\n';
                htmlBody += '    <div class="log-content">' + escapeHtml(errorLog.join('\n')) + '</div>\n';
                htmlBody += '  </div>\n';
            }

            // Footer
            htmlBody += '  <div class="footer">\n';
            htmlBody += '    <p><em>This is an automated notification from Snowflake Extract Table Creation Process.</em></p>\n';
            htmlBody += '    <p><em>For questions or issues, please contact the Data Engineering team.</em></p>\n';
            htmlBody += '  </div>\n';

            htmlBody += '</div>\n';
            htmlBody += '</body>\n';
            htmlBody += '</html>';

            // Send email using Snowflake's SYSTEM$SEND_EMAIL with proper MIME type
            var emailSQL = `
                CALL SYSTEM$SEND_EMAIL(
                    'EDM_STRATEGIC_SF_EMAIL_INTEGRATION',
                    '${recipient}',
                    '${subject.replace(/'/g, "''")}',
                    '${htmlBody.replace(/'/g, "''").replace(/\n/g, ' ')}',
                    'text/html'
                )
            `;

            var emailStmt = snowflake.createStatement({sqlText: emailSQL});
            emailStmt.execute();

        } catch (emailErr) {
            throw new Error("Email send failed: " + emailErr.message);
        }
    }

    // Helper function to escape HTML special characters
    function escapeHtml(text) {
        var map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, function(m) { return map[m]; });
    }
    
$$;



/*
-- Example usage with DEV environment:
CALL DEV_RAW_INBOUND_DB.CEDAR_GATE_RESP.CREATE_EXTRACT_TABLE_SP(
    'DEV_RAW_INBOUND_DB',      -- P_TARGET_DATABASE (structure source)
    'CEDAR_GATE_RESP',               -- P_TARGET_SCHEMA
    'ELIGIBILITY',              -- P_TABLE_NAME
    'DEV_AR_INBOUND_DB',         -- P_AR_INBOUND_DB (data source)
    'gxsajja@arkbluecross.com'   -- P_EMAIL_LIST
);
*/


/*
Update the PRD_RAW_INBOUND_DB.CEDAR_GATE_RESP.CEDAR_GATE_MASTR_FEEDBACK_PARALLEL_TABLE_LOAD_SP PROC to inlcude additional code to EXTRACT Tables. 

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

BEGIN
    -- STEP #1 Loop Through Each Configuration Row Where We Need To Trigger A Load

    FOR record IN C1 DO

        V_TARGET_DB_NAME          := RECORD.TARGET_DB_NAME;
        V_TARGET_SCHEMA_NAME      := RECORD.TARGET_SCHEMA_NAME;
        V_TARGET_TABLE_NAME       := RECORD.TARGET_TABLE_NAME;
        V_SOURCE_DB_NAME          := RECORD.SOURCE_DB_NAME;
        V_SOURCE_SCHEMA_NAME      := RECORD.SOURCE_SCHEMA_NAME;
        V_SOURCE_TABLE_NAME       := RECORD.SOURCE_TABLE_NAME;
        V_LOAD_TYPE               := RECORD.LOAD_TYPE;

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

        -- Execute additional procedure only for MONTHLY load type
        IF UPPER(V_LOAD_TYPE) = 'MONTHLY' THEN
            ASYNC(
                CALL CEDAR_GATE_RESP.CREATE_EXTRACT_TABLE_SP(
                    :V_TARGET_DB_NAME,           -- P_SOURCE_DATABASE (structure source)
                    :V_TARGET_SCHEMA_NAME,       -- P_SOURCE_SCHEMA
                    :V_SOURCE_TABLE_NAME,        -- P_TABLE_NAME
                    :V_SOURCE_DB_NAME,           -- P_AR_INBOUND_DB (data source)
                    'gxsajja@arkbluecross.com; fajones@arkbluecross.com; swagle@arkbluecross.com'   -- P_EMAIL_LIST
                )
            );
        END IF;

    END FOR;

    -- Wait for all async jobs to complete
    AWAIT ALL;

    RETURN 'Cedar Gate Table Archival is Complete';

END;
**/