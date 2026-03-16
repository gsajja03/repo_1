
CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_PRACTITIONER_ROLE_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
USER_TASK_TIMEOUT_MS = 36000000000
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_PRACTITIONER_ROLE_TEMP LIKE  PDT_FHIR_PRACTITIONER_ROLE;


   INSERT INTO  PDT_FHIR_PRACTITIONER_ROLE_TEMP 
(
     FILE_EXPORT_TYPE
    ,FILE_RECORD_ID
    ,VERSION_ID
    ,LAST_UPDTD_TS
    ,ORGANIZATION_REF_RECORD_ID
    ,PRACTITIONER_REF_RECORD_ID
    ,LICENSEE_REF_RECORD_ID
    ,NETWORK_REF_RECORD_ID
    ,LOCATION_REF_RECORD_ID
    ,HEALTHCARE_SERVICE_REF_RECORD_ID
    ,PROV_ENTITY_ID
    ,PROV_TYPE_CD
    ,PROV_TYPE_DISPLAY
    ,PROV_TYPE_SYSTEM
    ,PROV_ENTITY_TYPE_CD
    ,PROV_ENTITY_TYPE_DISPLAY
    ,PROV_ENTITY_TYPE_SYSTEM
    ,PATIENT_AGE_RANGE_HIGH
    ,PATIENT_AGE_RANGE_LOW
    ,PROV_PATIENT_GENDER_LIMIT
    ,PROV_EFF_DT
    ,PROV_TERM_DT
    ,ACCEPTING_NEW_PATIENTS_CD
    ,VALUE_BASED_PROGRAM_ID
    ,PCP_IND_CD
    ,PCP_EFF_DT
    ,PCP_END_DT
    ,PCP_SELECTABILITY_CD
    ,PROV_REIMBURSEMENT_CD
    ,PROV_TERMINATION_CD
    ,SPECIALTY_CD
    ,SPECIALTY_DISPLAY
    ,SPECIALTY_SYSTEM
    ,DAYS_OF_WEEK_AND_OFFICE_HOURS
    ,PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND
    ,DISPLAY_IND_PROV
    ,DISPLAY_SUPPRESSION_REASON_PROV
    ,PROV_CONTRACT_STATUS
    ,PROV_AFFIL_TYPE
    ,PROV_AFFIL_REF
    ,PROV_AFFIL_NETWORK
    ,TIER_DESIGNATION_CD,PROFILE_TYPE, LOAD_DT
)

SELECT 
     FILE_EXPORT_TYPE
    ,FILE_RECORD_ID
    ,VERSION_ID
    ,LAST_UPDTD_TS
    ,ORGANIZATION_REF_RECORD_ID
    ,PRACTITIONER_REF_RECORD_ID
    ,LICENSEE_REF_RECORD_ID
    ,NETWORK_REF_RECORD_VEC.VALUE AS NETWORK_REF_RECORD_ID
    ,LOCATION_REF_RECORD_ID
    ,HEALTHCARE_SERVICE_REF_RECORD_ID
    ,PROV_ENTITY_ID
    ,PROV_TYPE_CD
    ,PROV_TYPE_DISPLAY
    ,PROV_TYPE_SYSTEM
    ,PROV_ENTITY_TYPE_CD
    ,PROV_ENTITY_TYPE_DISPLAY
    ,PROV_ENTITY_TYPE_SYSTEM
    ,PATIENT_AGE_RANGE_HIGH
    ,PATIENT_AGE_RANGE_LOW
    ,PROV_PATIENT_GENDER_LIMIT
    ,PROV_EFF_DT
    ,PROV_TERM_DT
    ,ACCEPTING_NEW_PATIENTS_CD
    ,VALUE_BASED_PROGRAM_ID
    ,PCP_IND_CD
    ,PCP_EFF_DT
    ,PCP_END_DT
    ,PCP_SELECTABILITY_CD
    ,PROV_REIMBURSEMENT_CD
    ,PROV_TERMINATION_CD
    ,SPECIALTY_CD
    ,SPECIALTY_DISPLAY
    ,SPECIALTY_SYSTEM
    ,DAYS_OF_WEEK_AND_OFFICE_HOURS
    ,PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND
    ,DISPLAY_IND_PROV
    ,DISPLAY_SUPPRESSION_REASON_PROV
    ,PROV_CONTRACT_STATUS
    ,PROV_AFFIL_TYPE
    ,provider_Affiliation_Ref AS PROV_AFFIL_REF
    ,provider_Affiliation_Network AS PROV_AFFIL_NETWORK
    ,TIER_DESIGNATION_CD AS TIER_DESIGNATION_CD
    ,PROFILE_TYPE
    ,LOAD_DT     
  FROM (
  
WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='PractitionerRole' 
GROUP BY FILE_RECORD_ID)

SELECT 
FILE_EXPORT_TYPE,
FILE_RECORD_ID,
MAX(VERSION_ID) AS VERSION_ID,
LAST_UPDTD AS LAST_UPDTD_TS,
MAX(ORGANIZATION_REF_RECORD_ID) AS ORGANIZATION_REF_RECORD_ID,
PRACTITIONER_REF_RECORD_ID,
MAX(licensee_reference_ID) AS LICENSEE_REF_RECORD_ID,
LISTAGG(DISTINCT CASE WHEN NETWORK_REFERENCE_ID <>'' THEN NETWORK_REFERENCE_ID END ,'||') AS NETWORK_REF_RECORD_ID,
location_ref_id AS LOCATION_REF_RECORD_ID,
HEALTHCARE_SERVICE_REF_RECORD_ID,
MAX(PROV_ID) AS PROV_ENTITY_ID,
MAX(PROV_TYPE_CODE) AS PROV_TYPE_CD,
MAX(PROV_TYPE_DISPLAY) AS PROV_TYPE_DISPLAY,
MAX(PROV_TYPE_SYSTEM) AS PROV_TYPE_SYSTEM,
MAX(PROV_ENTITY_TYPE_CODE) AS PROV_ENTITY_TYPE_CD,
MAX(PROV_ENTITY_TYPE_DISPLAY) AS PROV_ENTITY_TYPE_DISPLAY,
MAX(PROV_ENTITY_TYPE_SYSTEM) AS PROV_ENTITY_TYPE_SYSTEM,
MAX(Patient_age_range_high) AS PATIENT_AGE_RANGE_HIGH,
MAX(Patient_age_range_low) AS PATIENT_AGE_RANGE_LOW,
MAX(Provider_Patient_Gender_Limitation) AS PROV_PATIENT_GENDER_LIMIT,
MAX(PROVIDER_EFFECTIVE_DATE) AS PROV_EFF_DT,
MAX(PROVIDER_TERM_DATE) AS PROV_TERM_DT,
MAX(ACCEPTING_NEW_PATIENTS_CODE) AS ACCEPTING_NEW_PATIENTS_CD,
TRIM(LISTAGG(DISTINCT NVL(Value_Based_Program_Identifier,' '), ' ')) AS VALUE_BASED_PROGRAM_ID,
MAX(PCP_Indicator_Code) AS PCP_IND_CD,
MAX(PCP_Effective_date) AS PCP_EFF_DT,
MAX(PCP_End_date) AS PCP_END_DT,
MAX(pcp_Selectability_Code) AS PCP_SELECTABILITY_CD,
MAX(provider_Reimbursement_Code) AS PROV_REIMBURSEMENT_CD,
MAX(Provider_Termination_Code) AS PROV_TERMINATION_CD,
MAX(specialty_code) AS SPECIALTY_CD,
MAX(specialty_display) AS SPECIALTY_DISPLAY,
MAX(specialty_system) AS SPECIALTY_SYSTEM,
MAX(DAYS_OF_WEEK_AND_OFFICE_HOURS) AS DAYS_OF_WEEK_AND_OFFICE_HOURS,
MAX(provider_Accepts_Member_Select_Attribution_Indicator  ) AS PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND,
MAX(display_Indicator_Provider) AS DISPLAY_IND_PROV,
MAX(display_Suppression_Reason_Provider) AS DISPLAY_SUPPRESSION_REASON_PROV,
MAX(Provider_Contract_Status) AS PROV_CONTRACT_STATUS,
MAX(provider_Affiliation_Type) AS PROV_AFFIL_TYPE,
MAX(provider_Affiliation_Ref) AS provider_Affiliation_Ref,
MAX(provider_Affiliation_Network) AS provider_Affiliation_Network,
MAX(TIER_DESIGNATION_CD) AS TIER_DESIGNATION_CD ,MAX(PROFILE_TYPE) AS PROFILE_TYPE, LOAD_DT
FROM ( 
SELECT SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE
,A.FILE_RECORD_ID
,FILE_CONTENT_RAW:meta:versionId AS VERSION_ID
,FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD
,SPLIT_PART(FILE_CONTENT_RAW:practitioner.reference,'/',-1)     AS PRACTITIONER_REF_RECORD_ID
,SPLIT_PART(FILE_CONTENT_RAW:organization.reference,'/',-1)     AS ORGANIZATION_REF_RECORD_ID
,SPLIT_PART(location.value:reference,'/',-1)         AS location_ref_id
,SPLIT_PART(healthcareService.value:reference,'/',-1)   AS HEALTHCARE_SERVICE_REF_RECORD_ID
,identifier_type_coding.value:code AS PROV_ID_TYPE
,identifier.value:value  AS PROV_ID 
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'licenseereference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS licensee_reference_ID
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'networkreference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS network_reference_ID
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:code  ELSE '' END AS PROV_TYPE_CODE
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:display  ELSE '' END   AS PROV_TYPE_DISPLAY
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:system  ELSE '' END   AS PROV_TYPE_SYSTEM
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:code  ELSE '' END   AS PROV_ENTITY_TYPE_CODE
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:display  ELSE '' END   AS PROV_ENTITY_TYPE_DISPLAY
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:system  ELSE '' END   AS PROV_ENTITY_TYPE_SYSTEM
,extension.value:valueRange:high:value AS Patient_age_range_high
,extension.value:valueRange:low:value AS Patient_age_range_low
,CASE WHEN (extension_extension.value:url  = 'characteristics' ) THEN extension_extension.value:valueString ELSE '' END AS Provider_Patient_Gender_Limitation 
,FILE_CONTENT_RAW:period.start  AS PROVIDER_EFFECTIVE_DATE
,FILE_CONTENT_RAW:period.end  AS PROVIDER_TERM_DATE
,CASE WHEN (extension_extension.value:url ='acceptingPatients') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  ACCEPTING_NEW_PATIENTS_CODE
,CASE WHEN (extension_extension.value:url ='valueBasedProgramIdentifier') THEN extension_extension.value:valueIdentifier:value ELSE '' END  AS  Value_Based_Program_Identifier
,CASE WHEN (extension_extension.value:url ='pcpIndicatorCode') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS   PCP_Indicator_Code
,CASE WHEN (extension_extension.value:url ='pcpEffectivePeriod') THEN extension_extension.value:valuePeriod:start ELSE '' END  AS  PCP_Effective_date
,CASE WHEN (extension_extension.value:url ='pcpEffectivePeriod') THEN extension_extension.value:valuePeriod:end ELSE '' END  AS  PCP_End_date
,CASE WHEN (extension_extension.value:url ='pcpSelectabilityCode') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  pcp_Selectability_Code
,CASE WHEN (extension_extension.value:url ='providerReimbursementCode') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  provider_Reimbursement_Code
,CASE WHEN (extension_valueCodeableConcept_coding.value:system ='http://bpd.bcbs.com/CodeSystem/BCBSProviderTerminationReasonCS') THEN extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  Provider_Termination_Code
,CASE WHEN (extension_extension.value:url ='providerAcceptsMemberSelectAttributionIndicator') THEN extension_extension.value:valueBoolean ELSE '' END  AS  provider_Accepts_Member_Select_Attribution_Indicator
,CASE WHEN (extension_extension.value:url ='displayIndicatorProvider') THEN extension_extension.value:valueBoolean ELSE '' END  AS  display_Indicator_Provider  
,CASE WHEN (extension_extension.value:url ='displaySuppressionReasonProvider') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  display_Suppression_Reason_Provider  
,CASE WHEN (extension_extension.value:url ='provAffType') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  provider_Affiliation_Type  
,CASE WHEN (extension_extension.value:url ='provAffRef')     THEN SPLIT_PART(extension_extension.value:valueReference:reference,'/',-1) ELSE '' END  AS  provider_Affiliation_Ref
,CASE WHEN (extension_extension.value:url ='provAffNetwork') THEN SPLIT_PART(extension_extension.value:valueReference:reference,'/',-1) ELSE '' END  AS  provider_Affiliation_Network
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'provcontract') THEN extension.value:valueBoolean ELSE '' END AS Provider_Contract_Status 
,specialty_coding.value:code  AS specialty_code
,specialty_coding.value:display  AS specialty_display
,specialty_coding.value:system  AS specialty_system
, FILE_CONTENT_RAW:availableTime[0]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[0]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[0]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[1]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[1]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[1]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[2]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[2]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[2]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[3]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[3]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[3]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[4]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[4]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[4]:availableStartTime      AS DAYS_OF_WEEK_AND_OFFICE_HOURS
 ,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'provtierdesig') THEN extension_valueCodeableConcept_coding.value ELSE '' END AS  TIER_DESIGNATION_CD
 ,split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE 
 ,LOAD_DT
FROM PDT_FHIR A ,MAX_PDT_REC B
,lateral flatten(input=>FILE_CONTENT_RAW:location,outer=>true) location
,lateral flatten(input=>FILE_CONTENT_RAW:identifier,outer=>true) identifier
,lateral flatten(input=>identifier.value:type:coding,outer=>true) identifier_type_coding
,lateral flatten(input=>FILE_CONTENT_RAW:specialty,outer=>true) specialty
,lateral flatten(input=>specialty.value:coding,outer=>true) specialty_coding
,lateral flatten(input=>FILE_CONTENT_RAW:healthcareService,outer=>true) healthcareService
,lateral flatten(input=>FILE_CONTENT_RAW:code,outer=>true) code
,lateral flatten(input=>code.value:coding,outer=>true) code_coding
,lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension
,lateral flatten(input=>extension.value:extension,outer=>true) extension_extension
,lateral flatten(extension_extension.value:valueCodeableConcept:coding,outer=>true) extension_extension_valueCodeableConcept_coding
,lateral flatten(extension.value:valueCodeableConcept:coding,outer=>true)extension_valueCodeableConcept_coding
WHERE  FILE_RESOURCE_TYPE='PractitionerRole' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED
AND A.LOAD_DT  IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_PRACTITIONER_ROLE )  
 )
 GROUP BY FILE_EXPORT_TYPE
,FILE_RECORD_ID,
PRACTITIONER_REF_RECORD_ID, location_ref_id,
HEALTHCARE_SERVICE_REF_RECORD_ID,LOAD_DT, LAST_UPDTD_TS ) PR,
LATERAL FLATTEN(INPUT=>SPLIT(PR.NETWORK_REF_RECORD_ID,'||'),OUTER=>TRUE)  NETWORK_REF_RECORD_VEC;

MERGE INTO PDT_FHIR_PRACTITIONER_ROLE PR
USING PDT_FHIR_PRACTITIONER_ROLE_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_PRACTITIONER_ROLE  (SELECT * FROM PDT_FHIR_PRACTITIONER_ROLE_TEMP);

   END;

ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_PRACTITIONER_ROLE_LOAD_DAILY_TASK RESUME;
