
CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_ORGANIZATION_AFFLIATION_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
USER_TASK_TIMEOUT_MS = 36000000000
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 
   USE WAREHOUSE {{env}}_ELT_BIG_WH;

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_ORGANIZATION_AFFLIATION_TEMP LIKE  PDT_FHIR_ORGANIZATION_AFFLIATION;


INSERT INTO PDT_FHIR_ORGANIZATION_AFFLIATION_TEMP (
             FILE_EXPORT_TYPE
            ,FILE_RECORD_ID
            ,VERSION_ID
            ,LAST_UPDTD_TS
            ,NETWORK_REF_RECORD_ID
            ,LICENSEE_REF_RECORD_ID
            ,HEALTHCARE_SERVICE_REF_RECORD_ID
            ,LOCATION_REF_RECORD_ID
            ,PARTICIPATING_ORGANIZATION_REF_RECORD_ID
            ,PROV_TYPE_CD
            ,PROV_TYPE_DISPLAY
            ,PROV_TYPE_SYSTEM
            ,PROV_ENTITY_TYPE_CD
            ,PROV_ENTITY_TYPE_DISPLAY
            ,PROV_ENTITY_TYPE_SYSTEM
            ,PROV_EFF_DT
            ,PROV_TERM_DT
            ,PCP_IND_CD
            ,PCP_EFF_DT
            ,PCP_END_DT
            ,PROV_REIMBURSEMENT_CD
            ,PROV_TERMINATION_CD
            ,PCP_SELECTABILITY_CD
            ,DAYS_OF_WEEK_AND_OFFICE_HOURS
            ,VALUE_BASED_PROGRAM_ID_LIST
            ,PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND
            ,PROV_ENTITY_ID
            ,TIER_DESIGNATION_CD
            ,PROV_CONTRACT_STATUS
            ,DISPLAY_IND_PROV
            ,DISPLAY_SUPPRESSION_REASON_PROV
            ,PROV_AFFLIATION_TYPE
            ,AFFLIATION_DESC
            ,HEALTHCARE_SYSTEM_NAME
            ,HEALTHCARE_SYSTEM_CITY
            ,HEALTHCARE_SYSTEM_STATE
            ,PROFILE_TYPE
            ,LOAD_DT

)

SELECT 
            FILE_EXPORT_TYPE
            ,FILE_RECORD_ID
            ,VERSION_ID
            ,LAST_UPDTD_TS
            ,NETWORK_REF_RECORD_VEC.VALUE AS NETWORK_REF_RECORD_ID
            ,LICENSEE_REF_RECORD_ID
            ,HEALTHCARE_SERVICE_REF_RECORD_ID
            ,LOCATION_REF_RECORD_ID
            ,PARTICIPATING_ORGANIZATION_REF_RECORD_ID
            ,PROV_TYPE_CD
            ,PROV_TYPE_DISPLAY
            ,PROV_TYPE_SYSTEM
            ,PROV_ENTITY_TYPE_CD
            ,PROV_ENTITY_TYPE_DISPLAY
            ,PROV_ENTITY_TYPE_SYSTEM
            ,PROV_EFF_DT
            ,PROV_TERM_DT
            ,PCP_IND_CD
            ,PCP_EFF_DT
            ,PCP_END_DT
            ,PROV_REIMBURSEMENT_CD
            ,PROV_TERMINATION_CD
            ,PCP_SELECTABILITY_CD
            ,DAYS_OF_WEEK_AND_OFFICE_HOURS
            ,VALUE_BASED_PROGRAM_ID_LIST
            ,PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND
            ,PROV_ENTITY_ID
            ,TIER_DESIGNATION_CD
            ,PROV_CONTRACT_STATUS
            ,DISPLAY_IND_PROV
            ,DISPLAY_SUPPRESSION_REASON_PROV
            ,PROV_AFFLIATION_TYPE
            ,AFFLIATION_DESC
            ,HEALTHCARE_SYSTEM_NAME
            ,HEALTHCARE_SYSTEM_CITY
            ,HEALTHCARE_SYSTEM_STATE
            ,PROFILE_TYPE
            ,LOAD_DT
FROM (

WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='OrganizationAffiliation' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))


SELECT 
    FILE_EXPORT_TYPE                                
  , FILE_RECORD_ID 
  , MAX(VERSION_ID) AS VERSION_ID
  , MAX(LAST_UPDTD) AS LAST_UPDTD_TS
  --, MAX(NETWORK_REFERENCE_ID  )
  ,LISTAGG(DISTINCT CASE WHEN NETWORK_REFERENCE_ID <>'' THEN NETWORK_REFERENCE_ID END ,'||') AS NETWORK_REF_RECORD_ID
  , MAX(LICENSEE_REFERENCE_ID  ) AS LICENSEE_REF_RECORD_ID
  , HEALTHCARE_SERVICE_REF_RECORD_ID  
  , LOCATION_REF_ID AS LOCATION_REF_RECORD_ID
  , MAX(PARTICIPATING_ORGANIZATION_REFERENCE_ID  ) AS PARTICIPATING_ORGANIZATION_REF_RECORD_ID
  , MAX(PROV_TYPE_CODE  ) AS PROV_TYPE_CD
  , MAX(PROV_TYPE_DISPLAY  ) AS PROV_TYPE_DISPLAY
  , MAX(PROV_TYPE_SYSTEM  ) AS PROV_TYPE_SYSTEM
  , MAX(PROV_ENTITY_TYPE_CD  ) AS PROV_ENTITY_TYPE_CD
  , MAX(PROV_ENTITY_TYPE_DISPLAY  ) AS PROV_ENTITY_TYPE_DISPLAY
  , MAX(PROV_ENTITY_TYPE_SYSTEM  ) AS PROV_ENTITY_TYPE_SYSTEM
  , MAX(PROVIDER_EFFECTIVE_DATE  ) AS PROV_EFF_DT
  , MAX(PROVIDER_TERM_DATE  ) AS PROV_TERM_DT
  , MAX(PCP_INDICATOR_CODE  ) AS PCP_IND_CD
  , MAX(PCP_EFFECTIVE_DATE  ) AS PCP_EFF_DT
  , MAX(PCP_END_DATE  ) PCP_END_DT
  , MAX(PROVIDER_REIMBURSEMENT_CODE  ) PROV_REIMBURSEMENT_CD
  , MAX(PROVIDER_TERMINATION_CODE  ) PROV_TERMINATION_CD
  , MAX(PCP_SELECTABILITY_CODE  ) PCP_SELECTABILITY_CD
  , MAX(DAYS_OF_WEEK_AND_OFFICE_HOURS  ) DAYS_OF_WEEK_AND_OFFICE_HOURS
  --, MAX(VALUE_BASED_PROGRAM_IDENTIFIER  ) VALUE_BASED_PROGRAM_ID_LIST
  ,TRIM(LISTAGG(DISTINCT NVL(VALUE_BASED_PROGRAM_IDENTIFIER,' '), ' ')) AS VALUE_BASED_PROGRAM_ID_LIST
  , MAX(PROVIDER_ACCEPTS_MEMBER_SELECT_ATTRIBUTION_INDICATOR  ) PROV_ACCEPTS_MEMBER_SELECT_ATTRIB_IND
  , MAX(PROV_ENTITY_IDENTIIFER  ) PROV_ENTITY_ID
  , MAX(TIER_DESIGNATION  ) AS TIER_DESIGNATION_CD
  , MAX(PROVIDER_CONTRACT_STATUS  ) PROV_CONTRACT_STATUS
  , MAX(DISPLAY_INDICATOR_PROVIDER  ) DISPLAY_IND_PROV
  , MAX(DISPLAY_SUPPRESSION_REASON_PROVIDER  ) DISPLAY_SUPPRESSION_REASON_PROV
  , MAX(PROVIDER_AFFLIATION_TYPE  ) ||':'|| MAX(PROVIDER_AFFLIATION_REF) PROV_AFFLIATION_TYPE
  , MAX(AFFLIATION_DESCRIPTION  ) AFFLIATION_DESC
  , MAX(HEALTHCARE_SYSTEM_NAME  ) HEALTHCARE_SYSTEM_NAME
  , MAX(HEALTHCARE_SYSTEM_CITY  ) HEALTHCARE_SYSTEM_CITY
  , MAX(HEALTHCARE_SYSTEM_STATE ) HEALTHCARE_SYSTEM_STATE
  ,MAX(PROFILE_TYPE) AS PROFILE_TYPE
  ,LOAD_DT
  FROM (

SELECT SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE
,A.FILE_RECORD_ID
,FILE_CONTENT_RAW:meta:versionId AS VERSION_ID
,FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD
,SPLIT_PART(location.value:reference,'/',-1)         AS LOCATION_REF_ID				
,SPLIT_PART(healthcareService.value:reference,'/',-1)   AS HEALTHCARE_SERVICE_REF_RECORD_ID
,CASE WHEN identifier_type_coding.value:code ='PEI' THEN identifier.value:value ELSE '' END AS PROV_ENTITY_IDENTIIFER  
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'licenseereference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS LICENSEE_REFERENCE_ID			
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'networkreference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS NETWORK_REFERENCE_ID	

,SPLIT_PART(FILE_CONTENT_RAW:participatingOrganization.reference,'/',-1)     AS PARTICIPATING_ORGANIZATION_REFERENCE_ID 

,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:code  ELSE '' END AS PROV_TYPE_CODE
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:display  ELSE '' END   AS PROV_TYPE_DISPLAY
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderTypeCS')  THEN code_coding.value:system  ELSE '' END   AS PROV_TYPE_SYSTEM
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:code  ELSE '' END   AS PROV_ENTITY_TYPE_CD
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:display  ELSE '' END   AS PROV_ENTITY_TYPE_DISPLAY
,CASE WHEN (SPLIT_PART(code_coding.value:system,'/',-1) = 'BCBSProviderEntityTypeCS')  THEN code_coding.value:system  ELSE '' END   AS PROV_ENTITY_TYPE_SYSTEM
,FILE_CONTENT_RAW:period.start  AS PROVIDER_EFFECTIVE_DATE
,FILE_CONTENT_RAW:period.end  AS PROVIDER_TERM_DATE
,CASE WHEN (extension_extension.value:url ='pcpIndicatorCode') THEN extension_extension_valueCodeableConcept_coding.value:code ||':'||NVL(extension_extension_valueCodeableConcept_coding.value:display,'') ELSE '' END  AS   PCP_INDICATOR_CODE	
,CASE WHEN (extension_extension.value:url ='pcpEffectivePeriod') THEN extension_extension.value:valuePeriod:start ELSE '' END  AS  PCP_EFFECTIVE_DATE
,CASE WHEN (extension_extension.value:url ='pcpEffectivePeriod') THEN extension_extension.value:valuePeriod:end ELSE '' END  AS  PCP_END_DATE
,CASE WHEN (extension_extension.value:url ='providerReimbursementCode') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  PROVIDER_REIMBURSEMENT_CODE
--,CASE WHEN (extension_extension.value:url ='provterm') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  PROVIDER_TERMINATION_CODE
,CASE WHEN (extension_valueCodeableConcept_coding.value:system ='http://bpd.bcbs.com/CodeSystem/BCBSProviderTerminationReasonCS') THEN extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  PROVIDER_TERMINATION_CODE

,CASE WHEN (extension_extension.value:url ='pcpSelectabilityCode') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  PCP_SELECTABILITY_CODE
, FILE_CONTENT_RAW:availableTime[0]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[0]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[0]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[1]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[1]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[1]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[2]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[2]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[2]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[3]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[3]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[3]:availableStartTime || ','||
  FILE_CONTENT_RAW:availableTime[4]:daysOfWeek[0] ||':'||FILE_CONTENT_RAW:availableTime[4]:availableEndTime ||'-'||FILE_CONTENT_RAW:availableTime[4]:availableStartTime      AS DAYS_OF_WEEK_AND_OFFICE_HOURS

  ,CASE WHEN (extension_extension.value:url ='valueBasedProgramIdentifier') THEN extension_extension.value:valueIdentifier:value ELSE '' END  AS  Value_Based_Program_Identifier

  
,CASE WHEN (extension_extension.value:url ='providerAcceptsMemberSelectAttributionIndicator') THEN extension_extension.value:valueBoolean ELSE '' END  AS  PROVIDER_ACCEPTS_MEMBER_SELECT_ATTRIBUTION_INDICATOR
,CASE WHEN (extension_extension.value:url ='displayIndicatorProvider') THEN extension_extension.value:valueBoolean ELSE '' END  AS  DISPLAY_INDICATOR_PROVIDER  
,CASE WHEN (extension_extension.value:url ='displaySuppressionReasonProvider') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  DISPLAY_SUPPRESSION_REASON_PROVIDER  
,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'provcontract') THEN extension.value:valueBoolean ELSE '' END AS PROVIDER_CONTRACT_STATUS 

,CASE WHEN (extension_extension.value:url ='provAffType') THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END  AS  PROVIDER_AFFLIATION_TYPE ,
CASE WHEN (extension_extension.value:url ='provAffRef') THEN SPLIT_PART(extension_extension.value:valueReference:reference,'/',-1) ELSE '' END  AS  PROVIDER_AFFLIATION_REF  

,CASE WHEN (extension_extension.value:url ='provAffInfo') THEN extension_extension.value:value ELSE '' END  AS  AFFLIATION_DESCRIPTION

,CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'provtierdesig') THEN extension_valueCodeableConcept_coding.value ELSE '' END AS  TIER_DESIGNATION 

,'' AS HEALTHCARE_SYSTEM_NAME
,'' AS HEALTHCARE_SYSTEM_CITY
,'' AS HEALTHCARE_SYSTEM_STATE
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

WHERE FILE_RESOURCE_TYPE='OrganizationAffiliation' AND healthcareService.index <=4
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND A.LOAD_DT IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_ORGANIZATION_AFFLIATION )

)
GROUP BY FILE_EXPORT_TYPE
,FILE_RECORD_ID,LOCATION_REF_ID,
HEALTHCARE_SERVICE_REF_RECORD_ID,LOAD_DT) OA ,
LATERAL FLATTEN(INPUT=>SPLIT(OA.NETWORK_REF_RECORD_ID,'||'),OUTER=>TRUE)  NETWORK_REF_RECORD_VEC;


MERGE INTO PDT_FHIR_ORGANIZATION_AFFLIATION PR
USING PDT_FHIR_ORGANIZATION_AFFLIATION_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_ORGANIZATION_AFFLIATION  (SELECT * FROM PDT_FHIR_ORGANIZATION_AFFLIATION_TEMP);

   END;
$$;
ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_ORGANIZATION_AFFLIATION_LOAD_DAILY_TASK RESUME;
