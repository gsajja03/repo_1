CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_ORGANIZATION_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_ORGANIZATION_TEMP LIKE  PDT_FHIR_ORGANIZATION;


INSERT INTO PDT_FHIR_ORGANIZATION_TEMP(
	
           FILE_EXPORT_TYPE
          ,FILE_RECORD_ID
          ,VERSION_ID
          ,LAST_UPDTD_TS
          ,PROV_ORGANIZATION_NAME
          ,PLAN_PROV_ID
          ,PROV_MASTER_ID
          ,NATIONAL_PROV_ID_LIST
          ,CMS_CERTIFICATION_NUMBER
          ,PROV_TAX_ID
          ,PROV_TAX_ID_TYPE
          ,CROSS_REFERENCE_PLAN_PROV_ID
          ,CROSS_REFERENCE_PLAN_PROV_ID_EFF_DT
          ,CROSS_REFERENCE_PLAN_PROV_ID_END_DT
          ,OTHER_PLAN_PROV_ID
          ,PROV_ADDR_TYPE
          ,PROV_ADDR_DISTRICT
          ,PROV_ADDR_STATE
          ,PROV_ADDR_POSTAL_CD
          ,PROV_ADDR_COUNTRY
          ,PROV_ADDR_USE
          ,PROV_ADDR_TEXT
          ,PROV_ADDR_LINE1
          ,PROV_ADDR_LINE2
          ,PROV_ADDR_START
          ,PROV_ADDR_END
          ,PROV_COMMUNICATION_INFO_TYPE_PHONE
          ,PROV_COMMUNICATION_INFO_PHONE
          ,PROV_COMMUNICATION_INFO_PURPOSE_PHONE
          ,PROV_COMMUNICATION_INFO_TYPE_FAX
          ,PROV_COMMUNICATION_INFO_FAX
          ,PROV_COMMUNICATION_INFO_PURPOSE_FAX
          ,PROV_COMMUNICATION_INFO_TYPE_URL
          ,PROV_COMMUNICATION_INFO_URL
          ,PROV_COMMUNICATION_INFO_PURPOSE_URL
          ,LICENSEE_ENTITY_CD
          ,PLAN_CD
          ,LICENSEE_HIERARCHY_CD
          ,PROFILE_TYPE
          ,NETWORK_REF_RECORD_ID
          ,LICENSEE_REF_RECORD_ID
          ,LOAD_DT
)

SELECT   FILE_EXPORT_TYPE
          ,FILE_RECORD_ID
          ,VERSION_ID
          ,LAST_UPDTD_TS
          ,PROV_ORGANIZATION_NAME
          ,PLAN_PROV_ID
          ,PROV_MASTER_ID
          ,NATIONAL_PROV_ID_LIST
          ,CMS_CERTIFICATION_NUMBER
          ,PROV_TAX_ID
          ,PROV_TAX_ID_TYPE
          ,CROSS_REFERENCE_PLAN_PROV_ID
          ,CROSS_REFERENCE_PLAN_PROV_ID_EFF_DT
          ,CROSS_REFERENCE_PLAN_PROV_ID_END_DT
          ,OTHER_PLAN_PROV_ID
          ,PROV_ADDR_TYPE
          ,PROV_ADDR_DISTRICT
          ,PROV_ADDR_STATE
          ,PROV_ADDR_POSTAL_CD
          ,PROV_ADDR_COUNTRY
          ,PROV_ADDR_USE
          ,PROV_ADDR_TEXT
          ,PROV_ADDR_LINE1
          ,PROV_ADDR_LINE2
          ,PROV_ADDR_START
          ,PROV_ADDR_END
          ,PROV_COMMUNICATION_INFO_TYPE_PHONE
          ,PROV_COMMUNICATION_INFO_PHONE
          ,PROV_COMMUNICATION_INFO_PURPOSE_PHONE
          ,PROV_COMMUNICATION_INFO_TYPE_FAX
          ,PROV_COMMUNICATION_INFO_FAX
          ,PROV_COMMUNICATION_INFO_PURPOSE_FAX
          ,PROV_COMMUNICATION_INFO_TYPE_URL
          ,PROV_COMMUNICATION_INFO_URL
          ,PROV_COMMUNICATION_INFO_PURPOSE_URL
          ,LICENSEE_ENTITY_CD
          ,PLAN_CD
          ,LICENSEE_HIERARCHY_CD
          ,PROFILE_TYPE
          ,NETWORK_REF_RECORD_VEC.VALUE AS NETWORK_REF_RECORD_ID
          ,LICENSEE_REF_RECORD_ID
          ,LOAD_DT
FROM (          

WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='Organization' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))


  SELECT 
  FILE_EXPORT_TYPE,
  FILE_RECORD_ID,
  MAX(VERSION_ID) AS VERSION_ID,
  MAX(LAST_UPDTD) AS LAST_UPDTD_TS,
  MAX(Provider_Organization_name) AS PROV_ORGANIZATION_NAME,
  MAX(Plan_Provider_Identifier) AS PLAN_PROV_ID,
  MAX(Provider_Master_Identifier) AS PROV_MASTER_ID,
 TO_VARIANT(TRIM(LISTAGG(DISTINCT NVL(National_Provider_Identifier,''), ' '))) AS NATIONAL_PROV_ID_LIST,
 MAX(CMS_Certification_Number) AS CMS_CERTIFICATION_NUMBER,
 MAX(Provider_Tax_ID) AS PROV_TAX_ID,
 MAX(Provider_Tax_ID_TYPE) AS PROV_TAX_ID_TYPE,
 MAX(Cross_Reference_Plan_Provider_Identifier) AS CROSS_REFERENCE_PLAN_PROV_ID,
 MAX(Cross_Reference_Plan_Provider_Identifier_Effective_Date) AS CROSS_REFERENCE_PLAN_PROV_ID_EFF_DT,
 MAX(Cross_Reference_Plan_Provider_Identifier_End_Date) AS CROSS_REFERENCE_PLAN_PROV_ID_END_DT,
  (MAx(Other_Plan_Provider_Identifier)) AS OTHER_PLAN_PROV_ID,
 
  MAX(Provider_Address_Type) AS PROV_ADDR_TYPE,
  MAX(Provider_Address_District) AS PROV_ADDR_DISTRICT,
  MAX(Provider_Address_State) AS PROV_ADDR_STATE,
  MAX(Provider_Address_Postalcode) AS PROV_ADDR_POSTAL_CD,
  MAX(Provider_Address_Country) AS PROV_ADDR_COUNTRY,
  MAX(Provider_Address_Use) AS PROV_ADDR_USE,
  MAX(Provider_Address_Text) AS PROV_ADDR_TEXT,
  MAX(Provider_Address_Line1) AS PROV_ADDR_LINE1,
  MAX(Provider_Address_Line2) AS PROV_ADDR_LINE2,
  MAX(Provider_Address_start) AS PROV_ADDR_START,
  MAX(Provider_Address_end) AS PROV_ADDR_END,
 
 MAX(Provider_Communication_Information_Type_phone) AS PROV_COMMUNICATION_INFO_TYPE_PHONE,
 MAX(Provider_Communication_Information_phone) AS PROV_COMMUNICATION_INFO_PHONE,
 MAX(Provider_Communication_Information_Purpose_phone) AS PROV_COMMUNICATION_INFO_PURPOSE_PHONE,
 
 MAX(Provider_Communication_Information_Type_fax) AS PROV_COMMUNICATION_INFO_TYPE_FAX,
 MAX(Provider_Communication_Information_fax) AS PROV_COMMUNICATION_INFO_FAX,
 MAX(Provider_Communication_Information_Purpose_fax) AS PROV_COMMUNICATION_INFO_PURPOSE_FAX,
 
 MAX(Provider_Communication_Information_Type_url) AS PROV_COMMUNICATION_INFO_TYPE_URL,
 MAX(Provider_Communication_Information_url) AS PROV_COMMUNICATION_INFO_URL,
 MAX(Provider_Communication_Information_Purpose_url) AS PROV_COMMUNICATION_INFO_PURPOSE_URL,
 MAX(Licensee_Entity_Code) AS LICENSEE_ENTITY_CD,
 MAX(Plan_Code) AS PLAN_CD,
 MAX(Licensee_Hierarchy_Code) AS LICENSEE_HIERARCHY_CD,
 MAX(PROFILE_TYPE) AS PROFILE_TYPE,
 --MAX(network_reference_ID) AS network_reference_ID,
 LISTAGG(DISTINCT CASE WHEN network_reference_ID <>'' THEN network_reference_ID END ,'||') AS network_reference_ID,
 MAX(licensee_reference_ID) AS LICENSEE_REF_RECORD_ID,
 LOAD_DT

 FROM (
  
SELECT  
FILE_CONTENT_RAW:name as Provider_Organization_name,
A.FILE_RECORD_ID,
FILE_CONTENT_RAW:meta:versionId AS VERSION_ID,
FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD,
SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE,

CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'licenseereference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS licensee_reference_ID,
CASE WHEN (SPLIT_PART(extension.value:url,'/',-1) = 'networkreference') THEN SPLIT_PART(extension.value:valueReference:reference ,'/',-1) ELSE '' END AS network_reference_ID,


CASE WHEN (identifier_type_coding.value:code = 'PRN') THEN identifier.value:value END AS Plan_Provider_Identifier,
CASE WHEN (identifier_type_coding.value:code = 'PRM') THEN identifier.value:value END AS Provider_Master_Identifier,
CASE WHEN (identifier_type_coding.value:code = 'NPI') THEN identifier.value:value END AS National_Provider_Identifier,
CASE WHEN (identifier_type_coding.value:code = 'CCN') THEN identifier.value:value END AS CMS_Certification_Number,
CASE WHEN (identifier_type_coding.value:code = 'LEC') THEN identifier.value:value END AS Licensee_Entity_Code,
CASE WHEN (identifier_type_coding.value:code = 'PLAN') THEN identifier.value:value END AS Plan_Code,   
CASE WHEN (identifier_type_coding.value:code = 'LHC') THEN identifier.value:value END AS Licensee_Hierarchy_Code,  

CASE WHEN (identifier_type_coding.value:code IN ('SS','TAX','ITIN')) THEN identifier.value:value else '' END AS Provider_Tax_ID,
CASE WHEN (identifier_type_coding.value:code IN ('SS','TAX','ITIN')) THEN identifier_type_coding.value:code else '' END AS Provider_Tax_ID_TYPE, 
CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:value else '' END AS Cross_Reference_Plan_Provider_Identifier,
CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:period:start else '' END AS Cross_Reference_Plan_Provider_Identifier_Effective_Date ,
CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:period:end else '' END AS Cross_Reference_Plan_Provider_Identifier_End_Date,
   
CASE WHEN (identifier_type_coding.value:code IS NULL OR identifier_type_coding.value:code NOT IN ('PRN','PRM','NPI','CCN','SS','TAX','ITIN','PCRN')) THEN identifier.value:value END AS Other_Plan_Provider_Identifier,
   
address.value:type as Provider_Address_Type,
  
address.value:city as Provider_Address_City,
address.value:district as Provider_Address_District,
address.value:state as Provider_Address_State,
address.value:postalCode as Provider_Address_Postalcode,
address.value:country as Provider_Address_Country,
address.value:use as Provider_Address_Use,

address.value:text as Provider_Address_Text,
address.value:line[0] as Provider_Address_Line1,
address.value:line[1] as Provider_Address_Line2,
address.value:period:start as Provider_Address_start,
address.value:period:end as Provider_Address_end,

CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_phone,
CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:value else ' ' END as Provider_Communication_Information_phone,
CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:use else ' ' END  as Provider_Communication_Information_Purpose_phone,

CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_fax,
CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:value else ' ' END as Provider_Communication_Information_fax,
CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:use else ' ' END  as Provider_Communication_Information_Purpose_fax,
   
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_url,
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:value else ' ' END  as Provider_Communication_Information_url,
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:use else ' ' END  as Provider_Communication_Information_Purpose_url , 
split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE,
LOAD_DT
FROM "BCBSA"."PDT_FHIR" A ,MAX_PDT_REC B,


lateral flatten(input=>FILE_CONTENT_RAW:address,outer=>true) address,
lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension,
lateral flatten(input=>FILE_CONTENT_RAW:identifier,outer=>true) identifier,
lateral flatten(input=>identifier.value:type:coding,outer=>true) identifier_type_coding,
lateral flatten(input=>FILE_CONTENT_RAW:telecom,outer=>true) telecom
WHERE FILE_RESOURCE_TYPE='Organization' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND LOAD_DT  IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_ORGANIZATION )
)

GROUP BY FILE_RECORD_ID,FILE_EXPORT_TYPE,Provider_Address_Use,LOAD_DT) OA,
LATERAL FLATTEN(INPUT=>SPLIT(OA.network_reference_ID,'||'),OUTER=>TRUE)  NETWORK_REF_RECORD_VEC;


MERGE INTO PDT_FHIR_ORGANIZATION PR
USING PDT_FHIR_ORGANIZATION_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_ORGANIZATION  (SELECT * FROM PDT_FHIR_ORGANIZATION_TEMP);

   END;

ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_ORGANIZATION_LOAD_DAILY_TASK RESUME;
