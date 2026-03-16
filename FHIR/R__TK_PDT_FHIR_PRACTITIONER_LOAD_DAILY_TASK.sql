CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_PRACTITIONER_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_PRACTITIONER_TEMP LIKE  PDT_FHIR_PRACTITIONER;

   INSERT INTO PDT_FHIR_PRACTITIONER_TEMP(

      FILE_EXPORT_TYPE
      ,FILE_RECORD_ID
      ,VERSION_ID
      ,LAST_UPDTD_TS
      ,PLAN_PROV_ID_TYPE_LIST
      ,PLAN_PROV_ID
      ,NATIONAL_PROV_ID
      ,PROV_MASTER_ID
      ,CROSS_REFERENCE_PLAN_PROV_ID
      ,CROSS_REFERENCE_PLAN_PROV_ID_EFF_DT
      ,CROSS_REFERENCE_PLAN_PROV_ID_END_DT
      ,PROV_TAX_ID
      ,PROV_TAX_ID_TYPE
      ,TAXONOMY_CD
      ,TAXONOMY_DESC
      ,PROV_MEDICAL_SCHOOL
      ,PROV_MEDICAL_SCHOOL_YEAR
      ,PROV_INTERNSHIP
      ,PROV_RESIDENCY
      ,BOARD_CERTIFICATION_IND
      ,BOARD_CERTIFICATION_ISSUING_ORGANIZATION
      ,PROV_CREDENTIAL
      ,PROV_LICENSE_STATE
      ,PROV_LAST_NAME
      ,PROV_FIRST_NAME
      ,PROV_MIDDLE_INITIAL
      ,PROV_NAME_SUFFIX
      ,JOBTITLE
      ,BIRTH_DT
      ,PROV_GENDER
      ,ACTIVE_IND
      ,COMMUNICATION
      ,CITY
      ,COUNTRY
      ,ADDR_LINE1
      ,ADDR_LINE2
      ,STATE
      ,TYPE
      ,USE
      ,PROFILE_TYPE
      ,LOAD_DT
) 

WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='Practitioner' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))
SELECT FILE_EXPORT_TYPE
,FILE_RECORD_ID
,MAX(VERSION_ID) AS VERSION_ID
,MAX(LAST_UPDTD) AS LAST_UPDTD_TS
,TRIM(LISTAGG(DISTINCT NVL(Plan_Provider_Identifier_TYPE,''), ' ')) PLAN_PROV_ID_TYPE_LIST
,MAX(Plan_Provider_Identifier) AS PLAN_PROV_ID
,MAX(National_Provider_Identifier) AS NATIONAL_PROV_ID
,MAX(Provider_Master_Identifier) AS PROV_MASTER_ID
,MAX(Cross_Reference_Plan_Provider_Identifier) AS CROSS_REFERENCE_PLAN_PROV_ID
,MAX(Cross_Reference_Plan_Provider_Identifier_Effective_Date) AS CROSS_REFERENCE_PLAN_PROV_ID_EFF_DT
,MAX(Cross_Reference_Plan_Provider_Identifier_End_Date) AS CROSS_REFERENCE_PLAN_PROV_ID_END_DT
,MAX(Provider_Tax_ID) AS PROV_TAX_ID
,MAX(Provider_Tax_ID_TYPE) AS PROV_TAX_ID_TYPE
,REGEXP_REPLACE(TRIM(LISTAGG(DISTINCT NVL(Taxonomy_Code,''), ' ')),' +',' ') AS TAXONOMY_CD
--,REGEXP_REPLACE(TRIM(LISTAGG(DISTINCT '"'||NVL(Taxonomy_Code,'')||'-'||NVL(Taxonomy_Desc,'')||'"',''),'||'),' +',' ') AS TAXONOMY_DESC
,REGEXP_REPLACE(LISTAGG(DISTINCT CASE WHEN Taxonomy_Code <> ''  THEN  Taxonomy_Code||':'||NVL(Taxonomy_Desc,'')||'\n' ELSE '' END  ),' +',' ') AS TAXONOMY_DESC
,MAX(Provider_Medical_School) AS PROV_MEDICAL_SCHOOL
,MAX(Provider_Medical_School_Year) AS PROV_MEDICAL_SCHOOL_YEAR
,MAX(Provider_Internship) AS PROV_INTERNSHIP
,MAX(Provider_Residency) AS PROV_RESIDENCY
,REGEXP_REPLACE(LISTAGG(DISTINCT Taxonomy_Code||':'||board_certification_indicator , '||'),' +',' ') AS BOARD_CERTIFICATION_IND
,REGEXP_REPLACE(LISTAGG(DISTINCT Taxonomy_Code||':'||Board_Certification_Issuing_Organization,'||'),' +',' ') AS BOARD_CERTIFICATION_ISSUING_ORGANIZATION

, TO_VARIANT(MAX(Provider_Credential)      ) AS PROV_CREDENTIAL
, TO_VARIANT(MAX(Provider_License_State)   ) AS PROV_LICENSE_STATE
, TO_VARIANT(MAX(Provider_Last_Name)       ) AS PROV_LAST_NAME
, TO_VARIANT(MAX(Provider_First_Name)      ) AS PROV_FIRST_NAME
, TO_VARIANT(MAX(Provider_Middle_Initial)  ) AS PROV_MIDDLE_INITIAL
, TO_VARIANT(MAX(Provider_Name_Suffix)     ) AS PROV_NAME_SUFFIX
, TO_VARIANT(MAX(jobtitle)                 ) AS  JOBTITLE
, TO_VARIANT(MAX(birth_date)               ) AS  BIRTH_DT
, TO_VARIANT(MAX(Provider_Gender)          ) AS  BIRTH_DT 
, TO_VARIANT(MAX(ACTIVE_IND)               ) AS  ACTIVE_IND


,REGEXP_REPLACE(TRIM(LISTAGG(DISTINCT NVL(communication,''), '\n')),' +','') AS COMMUNICATION

, TO_VARIANT(MAX(city)             ) AS  CITY
, TO_VARIANT(MAX(country)          ) AS  COUNTRY
, TO_VARIANT(MAX(address_line1)    ) AS  ADDR_LINE1
, TO_VARIANT(MAX(address_line2)    ) AS  ADDR_LINE2
, TO_VARIANT(MAX(state)            ) AS  STATE
, TO_VARIANT(MAX(type)             ) AS  TYPE
, TO_VARIANT(MAX(use)              ) AS USE
, MAX(PROFILE_TYPE) AS PROFILE_TYPE
, LOAD_DT
FROM (


SELECT SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE 
,A.FILE_RECORD_ID 
,FILE_CONTENT_RAW:meta:versionId AS VERSION_ID
,FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD
,identifier_type_coding.value:code AS Plan_Provider_Identifier_TYPE
,CASE WHEN (identifier_type_coding.value:code = 'PRN') THEN identifier.value:value else '' END AS Plan_Provider_Identifier 
,CASE WHEN (identifier_type_coding.value:code = 'NPI') THEN identifier.value:value else '' END AS National_Provider_Identifier 
,CASE WHEN (identifier_type_coding.value:code = 'PRM') THEN identifier.value:value else '' END AS Provider_Master_Identifier 
,CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:value else '' END AS Cross_Reference_Plan_Provider_Identifier
,CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:period:start else '' END AS Cross_Reference_Plan_Provider_Identifier_Effective_Date 
,CASE WHEN (identifier_type_coding.value:code = 'PCRN') THEN identifier.value:period:end else '' END AS Cross_Reference_Plan_Provider_Identifier_End_Date
,CASE WHEN (identifier_type_coding.value:code IN ('SS','TAX','ITIN','EN')) THEN identifier.value:value else '' END AS Provider_Tax_ID
,CASE WHEN (identifier_type_coding.value:code IN ('SS','TAX','ITIN','EN')) THEN identifier_type_coding.value:code else '' END AS Provider_Tax_ID_TYPE
,CASE WHEN (SPLIT_PART(qualification_code_coding.value:system,'/',-1) ='provider-taxonomy') THEN qualification_code_coding.value:code else '' END AS Taxonomy_Code 
,CASE WHEN (SPLIT_PART(qualification_code_coding.value:system,'/',-1) ='provider-taxonomy') THEN qualification_code_coding.value:display else '' END AS Taxonomy_Desc 
,CASE WHEN (qualification_code_coding.value:code = 'MEDSCHL') THEN SPLIT_PART(qualification.value:issuer:reference,'/',-1) else '' END AS Provider_Medical_School
,CASE WHEN (qualification_code_coding.value:code = 'MEDSCHL') THEN qualification.value:period:end else '' END AS Provider_Medical_School_Year
,CASE WHEN (qualification_code_coding.value:code = 'INTERN') THEN SPLIT_PART(qualification.value:issuer:reference,'/',-1) else '' END AS Provider_Internship
,CASE WHEN (qualification_code_coding.value:code = 'RESIDEN') THEN SPLIT_PART(qualification.value:issuer:reference,'/',-1) else '' END AS Provider_Residency
,CASE WHEN (SPLIT_PART(qual_extension_code_extension.value:url,'/',-1) = 'boardcertificationindicator') THEN qual_extension_code_extension.value:valueBoolean ELSE '' END AS board_certification_indicator
,CASE WHEN (qual_extension_extension.value:valueCode= 'boardcert') THEN SPLIT_PART(qualification.value:issuer:reference,'/',-1) ELSE '' END AS Board_Certification_Issuing_Organization
,qualification_code_coding.value:code  AS Provider_Credential 
,qual_extension_code_extension_extension_valueCodeableConcept_coding.value:code AS  Provider_License_State
,name.value:family as Provider_Last_Name
,name.value:given[0] as Provider_First_Name
,name.value:given[1] as Provider_Middle_Initial
,name.value:suffix[0] as Provider_Name_Suffix
,name.value:extension:jobtitle as jobtitle
,FILE_CONTENT_RAW:birthDate as birth_date
,FILE_CONTENT_RAW:gender as Provider_Gender
,FILE_CONTENT_RAW:active AS ACTIVE_IND
,NVL(communication_coding.value:code || ' : '||communication_coding.value:display,' ') as communication
,address.value:city AS city
,address.value:country AS country
,address.value:line[0] AS address_line1
,address.value:line[1] AS address_line2
,address.value:postalCode AS postalCode
,address.value:state AS state
,address.value:type AS type
,address.value:use AS use
,split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE
,LOAD_DT
FROM "BCBSA"."PDT_FHIR" A ,MAX_PDT_REC B 
 
,lateral flatten(input=>FILE_CONTENT_RAW:communication,outer=>true) communication  
,lateral flatten(input=>communication.value:coding,outer=>true) communication_coding 
,lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension
,lateral flatten(input=>FILE_CONTENT_RAW:name,outer=>true) name
,lateral flatten(input=>FILE_CONTENT_RAW:identifier,outer=>true) identifier
,lateral flatten(input=>identifier.value:type:coding,outer=>true) identifier_type_coding
,lateral flatten(input=>FILE_CONTENT_RAW:qualification,outer=>true) qualification
,lateral flatten(input=>qualification.value:code:coding,outer=>true) qualification_code_coding
,lateral flatten(input=>qualification.value:code:extension,outer=>true) qual_extension_code_extension
,lateral flatten(input=>qual_extension_code_extension.value:extension,outer=>true) qual_extension_code_extension_extension
,lateral flatten(input=>qual_extension_code_extension_extension.value:valueCodeableConcept:coding,outer=>true) qual_extension_code_extension_extension_valueCodeableConcept_coding
,lateral flatten(input=>qualification.value:extension,outer=>true) qual_extension_extension
,lateral flatten(input=>FILE_CONTENT_RAW:address,outer=>true) address
WHERE FILE_RESOURCE_TYPE='Practitioner' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND A.LOAD_DT  IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_PRACTITIONER )    

 )
 
 GROUP BY FILE_EXPORT_TYPE,FILE_RECORD_ID,LOAD_DT;


MERGE INTO PDT_FHIR_PRACTITIONER PR
USING PDT_FHIR_PRACTITIONER_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_PRACTITIONER  (SELECT * FROM PDT_FHIR_PRACTITIONER_TEMP);
 END;

ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_PRACTITIONER_LOAD_DAILY_TASK RESUME;
