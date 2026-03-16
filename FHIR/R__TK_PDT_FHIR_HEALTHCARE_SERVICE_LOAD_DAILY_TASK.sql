CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_HEALTHCARE_SERVICE_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_HEALTHCARE_SERVICE_TEMP LIKE  PDT_FHIR_HEALTHCARE_SERVICE;


INSERT INTO PDT_FHIR_HEALTHCARE_SERVICE_TEMP (

  FILE_EXPORT_TYPE
 ,FILE_RECORD_ID
 ,VERSION_ID
 ,LAST_UPDTD_TS
 ,HEALTHCARE_SERVICE_TYPE_CD
 ,HEALTHCARE_SERVICE_TYPE
 ,PROV_TYPE_CD
 ,PROV_TYPE_DISPLAY
 ,PATIENT_AGE_RANGE_HIGH
 ,PATIENT_AGE_RANGE_LOW
 ,PROV_PATIENT_GENDER_LIMIT
 ,ACCEPTING_NEW_PATIENTS_CD
 ,SPECIALTY_CD
 ,SPECIALTY_DISPLAY
 ,PROFILE_TYPE
 ,LOAD_DT
)

WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='HealthcareService' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))


SELECT 
 FILE_EXPORT_TYPE
,FILE_RECORD_ID
,MAX(VERSION_ID) AS VERSION_ID
,MAX(LAST_UPDTD) AS LAST_UPDTD_TS
,HealthcareService_type_code    AS HEALTHCARE_SERVICE_TYPE_CD
,MAX(HealthcareService_type)    AS HEALTHCARE_SERVICE_TYPE
,MAX(PROV_TYPE_CODE)            AS PROV_TYPE_CD
,MAX(PROV_TYPE_DISPLAY)         AS PROV_TYPE_DISPLAY
,MAX(Patient_age_range_high)    AS PATIENT_AGE_RANGE_HIGH
,MAX(Patient_age_range_low)     AS PATIENT_AGE_RANGE_LOW
,MAX(Provider_Patient_Gender_Limitation) AS PROV_PATIENT_GENDER_LIMIT
,MAX(ACCEPTING_NEW_PATIENTS_CODE)       AS ACCEPTING_NEW_PATIENTS_CD
,MAX(specialty_code)            AS SPECIALTY_CD
,MAX(specialty_display)         AS SPECIALTY_DISPLAY
,MAX(PROFILE_TYPE) AS PROFILE_TYPE
,LOAD_DT

FROM (
SELECT 
 SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE
,A.FILE_RECORD_ID
,FILE_CONTENT_RAW:meta:versionId AS VERSION_ID
,FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:code  ELSE '' END AS PROV_TYPE_CODE
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:display  ELSE '' END   AS PROV_TYPE_DISPLAY
,extension.value:valueRange:high:value AS Patient_age_range_high
,extension.value:valueRange:low:value AS Patient_age_range_low
,CASE WHEN (extension_extension.value:url  = 'characteristics' ) THEN extension_extension.value:valueString ELSE '' END AS Provider_Patient_Gender_Limitation 
,CASE WHEN (extension_extension.value:url ='acceptingPatients') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  ACCEPTING_NEW_PATIENTS_CODE
,specialty_coding.value:code  AS specialty_code
,specialty_coding.value:display  AS specialty_display

,type_coding.value:code AS  HealthcareService_type_code
,type_coding.value:display AS  HealthcareService_type
,split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE
,LOAD_DT
FROM "BCBSA"."PDT_FHIR"  A ,MAX_PDT_REC B
,lateral flatten(input=>FILE_CONTENT_RAW:specialty,outer=>true) specialty
,lateral flatten(input=>specialty.value:coding,outer=>true) specialty_coding
,lateral flatten(input=>FILE_CONTENT_RAW:code,outer=>true) code
,lateral flatten(input=>code.value:coding,outer=>true) code_coding
,lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension
,lateral flatten(input=>extension.value:extension,outer=>true) extension_extension
,lateral flatten(extension_extension.value:valueCodeableConcept:coding,outer=>true) extension_extension_valueCodeableConcept_coding
,lateral flatten(input=>FILE_CONTENT_RAW:type,outer=>true) type
,lateral flatten(input=>type.value:coding,outer=>true) type_coding
 
WHERE FILE_RESOURCE_TYPE='HealthcareService' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND A.LOAD_DT IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_HEALTHCARE_SERVICE )
 ) 

GROUP BY FILE_EXPORT_TYPE
,FILE_RECORD_ID, HealthcareService_type_code,LOAD_DT;


MERGE INTO PDT_FHIR_HEALTHCARE_SERVICE PR
USING PDT_FHIR_HEALTHCARE_SERVICE_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_HEALTHCARE_SERVICE  (SELECT * FROM PDT_FHIR_HEALTHCARE_SERVICE_TEMP);

   END;

ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_HEALTHCARE_SERVICE_LOAD_DAILY_TASK RESUME;
