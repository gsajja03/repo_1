CREATE OR REPLACE PROCEDURE COMMON.GENERIC_POINT_SOLUTIONS_FEEDBACK_LOOP_LOAD_SP("TARGET_DB_NAME" VARCHAR(16777216), "TARGET_SCHEMA_NAME" VARCHAR(16777216), "TARGET_TABLE_NAME" VARCHAR(16777216), "SOURCE_FIELDS_LIST" VARCHAR(16777216), "SOURCE_STAGE_NAME" VARCHAR(16777216), "SOURCE_FILES_PATTERN" VARCHAR(16777216), "SOURCE_FILE_FORMAT" VARCHAR(16777216), "EMAIL_INTEGRATION" VARCHAR(16777216), "EMAIL_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try{

var v_db_name               = TARGET_DB_NAME; 
var v_table                 = TARGET_TABLE_NAME;
var v_schema                = TARGET_SCHEMA_NAME;
var v_select_fields_list    = SOURCE_FIELDS_LIST;
var v_stage                 = SOURCE_STAGE_NAME;
var v_pattern               = SOURCE_FILES_PATTERN;
var v_file_format           = SOURCE_FILE_FORMAT;

//Set attributes for email notification
var v_integration   = EMAIL_INTEGRATION;
var v_distribution  = EMAIL_LIST;

var v_subject = "Point Solutions - Feedback Loop File Load Notification: "+v_db_name+"."+v_schema+"."+v_table+".";
var v_common_db = "COMMON_DB";

if(v_db_name.startsWith("TST") || v_db_name.startsWith("DEV")) 
  { 
    v_common_db =  "TST_COMMON_DB" ;
  }

//Copy files from stage to table
var sql_cmd = " COPY INTO "+v_db_name+"."+v_schema+"."+ v_table+" FROM ("+
    "SELECT "+v_select_fields_list+
     " FROM "+ v_stage +
          "(FILE_FORMAT =>"+v_file_format+" )T)"+
          "PATTERN= ''"+ v_pattern +"''"+
          "ON_ERROR =CONTINUE;";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        
   

       
//Record query ID
        var sql_cmd="select last_query_id()";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        var v_query_id = result_set1.getColumnValue(1);

 snowflake.createStatement( {sqlText: "COMMIT;"} ).execute();
     
//Record Load Status (create table or insert)
        var sql_cmd=`select * from table(result_scan(''`+v_query_id+`''));`
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        
        

        if(result_set1.getColumnValue(1) ==''Copy executed with 0 files processed.'') 
        {
        var v_result = ''Copy executed with 0 files processed.''
        }
        else 
        {
        var sql_cmd="INSERT INTO "+v_common_db+".AUDIT.FEEDBACK_LOAD_STATUS (TABLE_NAME, TARGET_DB,  TARGET_SCHEMA,  LOAD_DT, QUERY_ID, FILE, STATUS, ROWS_PARSED, ROW_LOADED, ERROR_LIMIT, ERRORS_SEEN, FIRST_ERROR, FIRST_ERROR_LINE, FIRST_ERROR_CHARACTER, FIRST_ERROR_COLUMN_NAME) select ''"+v_table+"''"+
        " AS TABLE_NAME,''"+v_db_name+"'' AS TARGET_DB, ''"+v_schema+"'' AS TARHET_SCHEMA, CURRENT_DATE() AS LOAD_DT,''"+v_query_id+"'' AS QUERY_ID , A.*  from table(result_scan(''"+v_query_id+"''))A;";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        v_result = ''Table Loaded Successfully, Please verify FEEDBACK_LOAD_STATUS with QUERY_ID''+v_query_id+'' for load status.'';
        
        }
       
//Validate copy. 
    var sql_cmd = "INSERT INTO "+v_common_db+".AUDIT.FEEDBACK_ERROR (TABLE_NAME, TARGET_DB, TARGET_SCHEMA,LOAD_TS, QUERY_ID, ERROR, FILE, LINE, CHARACTER, BYTE_OFFSET, CATEGORY, CODE, SQL_STATE, COLUMN_NAME, ROW_NUMBER, ROW_START_LINE, REJECTED_RECORD, FIXED_STATUS, ACTION_TAKEN) select ''"+v_table+"'' AS TABLE_NAME,''"+v_db_name+"'' AS TARGET_DB, ''"+v_schema+"'' AS TARGET_SCHEMA, CURRENT_TIMESTAMP() AS LOAD_TS,''"+v_query_id+"'' AS QUERY_ID , A.*,'' '','' ''  from table(validate("+v_db_name+"."+v_schema+"."+v_table+", job_id=>''"+v_query_id+"''))A;";

        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
 
      
 
   //Verify if errors are detected
        var sql_cmd="select count(*) from "+v_common_db+".AUDIT.FEEDBACK_ERROR WHERE QUERY_ID=''"+v_query_id+"'';";
        var statement1 = snowflake.createStatement( {sqlText: sql_cmd} );
        var result_set1 = statement1.execute();
        result_set1.next()
        var err_count = result_set1.getColumnValue(1);

 
//Buid a message based on errors
       if (err_count>0) 
       {
       var v_result = "Not all Records/Files are loaded into "+v_table+", please verify FEEDBACK_ERROR with QUERY_ID''"+v_query_id+ "'' for error details;"
       }
 
 
 if(v_result !=''Copy executed with 0 files processed.'') 
 {   


     var config_record_set = snowflake.execute({sqlText: "SELECT FILE , ROW_LOADED FROM "+v_common_db+".AUDIT.FEEDBACK_LOAD_STATUS  WHERE  QUERY_ID = ''"+v_query_id+ "''"});
     var v_file_list = "\\t";
    while (config_record_set.next()) 
     {
       v_file_list = v_file_list + "\\n \\t"+ config_record_set.getColumnValue(1) + "  : "+ config_record_set.getColumnValue(2) + " Records.";
     }

 
var v_result_1 = v_result+"\\n\\n \\t"+v_db_name+"."+v_schema+"."+v_table+" Table Loaded. \\n \\n Below Files are Loaded : "+v_file_list+" \\n\\n- Thanks."
const results = snowflake.execute({
            sqlText: `call SYSTEM$SEND_EMAIL(?,?,?,?)`,
            binds: [v_integration, v_distribution,v_subject, v_result_1 ],
            });            
        results.next();
            
 }        
       
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

