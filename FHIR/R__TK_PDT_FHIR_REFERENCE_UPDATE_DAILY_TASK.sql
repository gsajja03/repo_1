CREATE OR REPLACE TASK {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_REFERENCE_UPDATE_DAILY_TASK
WAREHOUSE = {{env}}ELT_BIG_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_INSURANCE_PLAN_DIM-----------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_INSURANCE_PLAN_DIM;
INSERT INTO BCBSA.PDT_FHIR_INSURANCE_PLAN_DIM(
INSURANCE_PLAN_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
INSURANCE_PLAN_NAME,
MEMPREFIX,
NETWORK_ID,
LICENSEE_ID,
INSURANCE_PLAN_CD,
INSURANCE_PLAN_CD_DESC,
PRODUCT_TYPE_CD,
PRODUCT_TYPE_CD_DESC,
PROGRAM_AFFL_TYPE_CD,
PROGRAM_AFFL_TYPE_CD_DESC,
LOAD_DT)

SELECT
INSURANCE_PLAN_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
INSURANCE_PLAN_NAME,
MEMPREFIX,
NETWORK_ID,
LICENSEE_ID,
INSURANCE_PLAN_CD,
INSURANCE_PLAN_CD_DESC,


MAX(PRODUCT_TYPE_CD) AS PRODUCT_TYPE_CD,
MAX(PRODUCT_TYPE_CD_DESC) AS PRODUCT_TYPE_CD_DESC,
MAX(PROGRAM_AFFL_TYPE_CD) AS PROGRAM_AFFL_TYPE_CD,
MAX(PROGRAM_AFFL_TYPE_CD_DESC) AS PROGRAM_AFFL_TYPE_CD_DESC,
CURRENT_DATE AS LOAD_DT
FROM 
(
SELECT 
entry.value:resource.id AS INSURANCE_PLAN_ID
,entry.value:resource.name AS INSURANCE_PLAN_NAME
,CASE WHEN SPLIT_PART(extension.value:url ,'/',-1) = 'memprefix'  THEN extension.value:valueString ELSE '' END AS MEMPREFIX

,network.value:reference AS NETWORK_ID
,SPLIT_PART(entry.value:resource.ownedBy.reference,'/',-1) AS LICENSEE_ID
,SPLIT_PART(identifier.value:value,'/',-1) AS INSURANCE_PLAN_CD
,identifier.value:assigner:display AS INSURANCE_PLAN_CD_DESC
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE

,CASE WHEN SPLIT_PART(type_coding.value:system ,'/',-1) = 'BCBSProductTypeCS'  THEN type_coding.value:code ELSE '' END AS PRODUCT_TYPE_CD
,CASE WHEN SPLIT_PART(type_coding.value:system ,'/',-1) = 'BCBSProductTypeCS'  THEN type_coding.value:display ELSE '' END AS PRODUCT_TYPE_CD_DESC

,CASE WHEN SPLIT_PART(type_coding.value:system ,'/',-1) = 'BCBSProgramAffiliationTypeCS'  THEN type_coding.value:code ELSE '' END AS PROGRAM_AFFL_TYPE_CD
,CASE WHEN SPLIT_PART(type_coding.value:system ,'/',-1) = 'BCBSProgramAffiliationTypeCS'  THEN type_coding.value:display ELSE '' END AS PROGRAM_AFFL_TYPE_CD_DESC

FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource.network,outer=>true) network
,lateral flatten(entry.value:resource.extension,outer=>true) extension
,lateral flatten(entry.value:resource.identifier,outer=>true) identifier
,lateral flatten(entry.value:resource:type, outer=>true) type
,lateral flatten(type.value:coding, outer=>true) type_coding
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND SPLIT_PART(FILE_NAME,'/',5)= 'InsurancePlan' --and file_name like '%InsurancePlan_20240127224954075%'
)
GROUP BY 
INSURANCE_PLAN_ID,
INSURANCE_PLAN_NAME,
MEMPREFIX,
NETWORK_ID,
LICENSEE_ID,
INSURANCE_PLAN_CD,
INSURANCE_PLAN_CD_DESC,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED)
QUALIFY  ROW_NUMBER() OVER (PARTITION BY INSURANCE_PLAN_ID, INSURANCE_PLAN_NAME,MEMPREFIX,NETWORK_ID,LICENSEE_ID,INSURANCE_PLAN_CD,INSURANCE_PLAN_CD_DESC,RESOURCE_TYPE  ORDER BY LAST_UPDATED DESC) = 1
ORDER BY INSURANCE_PLAN_ID;

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_LICENSEE_DIM-----------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_LICENSEE_DIM; 
INSERT INTO BCBSA.PDT_FHIR_LICENSEE_DIM(
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PLAN_CD,
PLAN_CD_DESC,
LICENSEE_HIERARCHY_CD,
LICENSEE_HIERARCHY_CD_DESC,
LICENSEE_ENTITY_CD,
LICENSEE_ENTITY_CD_DESC,
LOAD_DT
)
SELECT 
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
MAX(PLAN_CD) AS PLAN_CD,
MAX(PLAN_CD_DESC) AS PLAN_CD_DESC,
MAX(LICENSEE_HIERARCHY_CD) AS LICENSEE_HIERARCHY_CD,
MAX(LICENSEE_HIERARCHY_CD_DESC) AS LICENSEE_HIERARCHY_CD_DESC,
MAX(LICENSEE_ENTITY_CD) AS LICENSEE_ENTITY_CD,
MAX(LICENSEE_ENTITY_CD_DESC) AS LICENSEE_ENTITY_CD_DESC,
CURRENT_DATE AS LOAD_DT
FROM (
SELECT 
entry.value:resource.id AS INSURANCE_LICENSEE_ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'PLAN'  THEN identifier.value:value ELSE '' END AS PLAN_CD
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'PLAN'  THEN identifier.value:assigner:display ELSE '' END AS PLAN_CD_DESC
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'LHC'  THEN identifier.value:value ELSE '' END AS LICENSEE_HIERARCHY_CD
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'LHC'  THEN identifier.value:assigner:display ELSE '' END AS LICENSEE_HIERARCHY_CD_DESC
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'LEC'  THEN identifier.value:value ELSE '' END AS LICENSEE_ENTITY_CD
,CASE WHEN SPLIT_PART(type_coding.value:code ,'/',-1) = 'LEC'  THEN identifier.value:assigner:display ELSE '' END AS LICENSEE_ENTITY_CD_DESC
FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource.identifier,outer=>true) identifier
,lateral flatten(identifier.value:type:coding, outer=>true) type_coding
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'Licensee' 
)
GROUP BY 

INSURANCE_LICENSEE_ID,
VERSION_ID,
LAST_UPDATED,
RESOURCE_TYPE
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED)
QUALIFY  ROW_NUMBER() OVER (PARTITION BY INSURANCE_LICENSEE_ID,RESOURCE_TYPE  ORDER BY LAST_UPDATED DESC) = 1
ORDER BY INSURANCE_LICENSEE_ID;

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_NETWORK_DIM------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_NETWORK_DIM;
INSERT INTO BCBSA.PDT_FHIR_NETWORK_DIM(
ORGANIZATION_ID,
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PRODUCT_ID,
PRODUCT_ID_DESC,
PERIOD_END,
PERIOD_START,
PRODUCT_CD,
PRODUCT_CD_DESC,
PROV_PRODUCT_TYPE_CD,
PROV_PRODUCT_TYPE_CD_DESC,
PROV_PROGRAM_TYPE_CD,
PROV_PROGRAM_TYPE_CD_DESC,
LOAD_DT
)
SELECT 
ORGANIZATION_ID,
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PRODUCT_ID,
PRODUCT_ID_DESC,
PERIOD_END,
PERIOD_START,
SPLIT(SPIT_TAB.VALUE,'|')[0] PRODUCT_CD,
SPLIT(SPIT_TAB.VALUE,'|')[1]PRODUCT_CD_DESC,
PROV_PRODUCT_TYPE_CD,
PROV_PRODUCT_TYPE_CD_DESC,
PROV_PROGRAM_TYPE_CD,
PROV_PROGRAM_TYPE_CD_DESC,
LOAD_DT
FROM (
SELECT 
ORGANIZATION_ID,
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PRODUCT_ID,
MAX(PRODUCT_ID_DESC) AS PRODUCT_ID_DESC,
MAX(PERIOD_END) AS PERIOD_END,
MAX(PERIOD_START) AS PERIOD_START,
ARRAY_AGG(PRODUCT_CD||'|'||PRODUCT_CD_DESC) AS PRODUCT_CD_PRODUCT_CD_DESC,
--MAX(PRODUCT_CD) AS PRODUCT_CD,
--MAX(PRODUCT_CD_DESC) AS PRODUCT_CD_DESC,
MAX(PROV_PRODUCT_TYPE_CD ) AS PROV_PRODUCT_TYPE_CD,
MAX(PROV_PRODUCT_TYPE_CD_DESC) AS PROV_PRODUCT_TYPE_CD_DESC,
MAX(PROV_PROGRAM_TYPE_CD) AS PROV_PROGRAM_TYPE_CD,
MAX(PROV_PROGRAM_TYPE_CD_DESC) AS PROV_PROGRAM_TYPE_CD_DESC,
CURRENT_DATE AS LOAD_DT
FROM(

SELECT 
entry.value:resource.id AS ORGANIZATION_ID
,SPLIT_PART(entry.value:resource.partOf:reference,'/',-1) AS INSURANCE_LICENSEE_ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
,identifier.value:value AS PRODUCT_ID
,identifier.value:assigner:display AS PRODUCT_ID_DESC
,identifier.value:period:end AS PERIOD_END
,identifier.value:period:start AS PERIOD_START

,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'productCode'  THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END AS PRODUCT_CD
,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'productCode'  THEN extension_extension_valueCodeableConcept_coding.value:display ELSE '' END AS PRODUCT_CD_DESC
,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'provProdType'  THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END AS PROV_PRODUCT_TYPE_CD
,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'provProdType'  THEN extension_extension_valueCodeableConcept_coding.value:display ELSE '' END AS PROV_PRODUCT_TYPE_CD_DESC
,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'provPgmType'  THEN extension_extension_valueCodeableConcept_coding.value:code ELSE '' END AS PROV_PROGRAM_TYPE_CD
,CASE WHEN SPLIT_PART(extension_extension.value:url ,'/',-1) = 'provPgmType'  THEN extension_extension_valueCodeableConcept_coding.value:display ELSE '' END AS PROV_PROGRAM_TYPE_CD_DESC
FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource:extension,outer=>true) extension
,lateral flatten(entry.value:resource.identifier,outer=>true) identifier
,lateral flatten(extension.value:extension,outer=>true) extension_extension
,lateral flatten(extension_extension.value:valueCodeableConcept:coding,outer=>true) extension_extension_valueCodeableConcept_coding
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'Network' )

GROUP BY 
ORGANIZATION_ID,
INSURANCE_LICENSEE_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PRODUCT_ID
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED)
QUALIFY  ROW_NUMBER() OVER (PARTITION BY ORGANIZATION_ID,INSURANCE_LICENSEE_ID,RESOURCE_TYPE, PRODUCT_ID  ORDER BY LAST_UPDATED DESC) = 1
)  , LATERAL FLATTEN(INPUT=>PRODUCT_CD_PRODUCT_CD_DESC) SPIT_TAB WHERE PRODUCT_CD <> '' ;



------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_HEALTHCARE_SYSTEM_DIM--------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_HEALTHCARE_SYSTEM_DIM;

INSERT INTO BCBSA.PDT_FHIR_HEALTHCARE_SYSTEM_DIM (
HEALTHCARE_SYSTEM_ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
HEALTHCARE_SYSTEM_IDENTFIER,
HEALTHCARE_SYSTEM_IDENTFIER_TYPE,
HEALTHCARE_SYSTEM_NAME,
ADDRESS_CITY,
ADDRESS_STATE,
LOAD_DT)

SELECT 
entry.value:resource.id AS HEALTHCARE_SYSTEM_ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
,identifier.value:value AS HEALTHCARE_SYSTEM_IDENTFIER
,identifier.value:type:coding[0]:code AS HEALTHCARE_SYSTEM_IDENTFIER_TYPE
,entry.value:resource:name AS HEALTHCARE_SYSTEM_NAME
,address.value:city AS ADDRESS_CITY
,address.value:state AS ADDRESS_STATE
,CURRENT_DATE AS LOAD_DT
FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource:address,outer=>true) address
,lateral flatten(entry.value:resource.identifier,outer=>true) identifier
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'HealthcareSystem'
QUALIFY  ROW_NUMBER() OVER (PARTITION BY HEALTHCARE_SYSTEM_ID  ORDER BY LAST_UPDATED DESC) = 1
ORDER BY HEALTHCARE_SYSTEM_ID;

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_ST_COUNTY_ZIP_DIM------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_ST_COUNTY_ZIP_DIM;
INSERT INTO BCBSA.PDT_FHIR_ST_COUNTY_ZIP_DIM(
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
ZIP_CD,
LONGITUDE,
LATITUDE,
CITY,
COUNTY_CD,
COUNTY_NAME,
STATE_FIPS_CD,
CBSA_CD,
CBSA_NAME,
STATE_CD,
STATE_NAME,
LOAD_DT
)
SELECT 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
MAX(ZIP_CD) AS ZIP_CD,
MAX(LONGITUDE) AS LONGITUDE,
MAX(LATITUDE) AS LATITUDE,
MAX(CITY) AS CITY,
MAX(COUNTY_CD) AS COUNTY_CD,
MAX(COUNTY_NAME) AS COUNTY_NAME,
MAX(STAET_FIPS_CD) AS STATE_FIPS_CD,
MAX(CBSA_CD) AS CBSA_CD,
MAX(CBSA_NAME) AS CBSA_NAME,
MAX(STATE_CD) AS STATE_CD,
MAX(STATE_NAME)  AS STATE_NAME,
CURRENT_DATE AS LOAD_DT
FROM (
SELECT 
entry.value:resource.id AS ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
--,extension.value AS extension
--,extension.value:url AS StateCountyZipCode_type
,extension_valueCodeableConcept_coding.value as extension_valueCodeableConcept_coding
,CASE WHEN extension.value:url = 'zipCode' THEN extension.value:valueCode ELSE '' END AS ZIP_CD
,CASE WHEN extension.value:url = 'longitude' THEN extension.value:valueCode ELSE '' END AS LONGITUDE
,CASE WHEN extension.value:url = 'latitude' THEN extension.value:valueCode ELSE '' END AS LATITUDE
,CASE WHEN extension.value:url = 'cityName' THEN extension.value:valueCode ELSE '' END AS CITY
,CASE WHEN extension.value:url = 'countyCode' THEN extension_valueCodeableConcept_coding:code ELSE '' END AS COUNTY_CD
,CASE WHEN extension.value:url = 'countyCode' THEN extension_valueCodeableConcept_coding:display ELSE '' END AS COUNTY_NAME
,CASE WHEN extension.value:url = 'stateFipsCode' THEN extension.value:valueCode ELSE '' END AS STAET_FIPS_CD
,CASE WHEN extension.value:url = 'cbsaCode' THEN extension_valueCodeableConcept_coding:code ELSE '' END AS CBSA_CD
,CASE WHEN extension.value:url = 'cbsaCode' THEN extension_valueCodeableConcept_coding:display ELSE '' END AS CBSA_NAME
,CASE WHEN extension.value:url = 'stateCode' THEN extension_valueCodeableConcept_coding:code ELSE '' END AS STATE_CD
,CASE WHEN extension.value:url = 'stateCode' THEN extension_valueCodeableConcept_coding:display ELSE '' END AS STATE_NAME

FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource:extension,outer=>true) extension
,lateral flatten(extension.value:valueCodeableConcept:coding,outer=>true) extension_valueCodeableConcept_coding
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'StateCountyZipCode')

GROUP BY 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED)
QUALIFY  ROW_NUMBER() OVER (PARTITION BY ID,RESOURCE_TYPE  ORDER BY LAST_UPDATED DESC) = 1;

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_PROV_ENTITY_DIM--------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_PROV_ENTITY_DIM;
INSERT INTO BCBSA.PDT_FHIR_PROV_ENTITY_DIM(
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
PLAN_CD,
VBP_ID,
PEI_CD,
PEI_CD_NAME,
CYCLE_ID,
PEI_EFF_PERIOD_START,
PEI_EFF_PERIOD_END,
ADMIN_ACT,
LOAD_DT
)
SELECT 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
MAX(PLAN_CD) AS PLAN_CD,
MAX(VBP_ID) AS VBP_ID,
MAX(PEI_CD) AS PEI_CD,
MAX(PEI_CD_NAME) AS PEI_CD_NAME,
MAX(CYCLE_ID) AS CYCLE_ID,
TRY_TO_DATE(MAX(PEI_EFF_PERIOD_START)) AS PEI_EFF_PERIOD_START,
TRY_TO_DATE(MAX(PEI_EFF_PERIOD_END)) AS PEI_EFF_PERIOD_END,
ADMIN_ACT,
CURRENT_DATE AS LOAD_DT
FROM (
SELECT 
entry.value:resource.id AS ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
,extension_valueCodeableConcept_coding.value as extension_valueCodeableConcept_coding

,CASE WHEN extension.value:url = 'planCode' THEN extension.value:valueCode  WHEN extension_extension.value:url = 'planCode' THEN extension_extension.value:valueCode ELSE '' END AS PLAN_CD
,CASE WHEN extension.value:url = 'vbpId' THEN extension.value:valueCode     WHEN extension_extension.value:url = 'vbpId' THEN extension_extension.value:valueCode    ELSE '' END AS VBP_ID
,CASE WHEN extension.value:url = 'peiCode' THEN extension_valueCodeableConcept_coding:code  ELSE '' END AS PEI_CD
,CASE WHEN extension.value:url = 'peiCode' THEN extension_valueCodeableConcept_coding:display  ELSE '' END AS PEI_CD_NAME
,CASE WHEN extension.value:url = 'cycleId' THEN extension.value:valueCode   WHEN extension_extension.value:url = 'cycleId' THEN extension_extension.value:valueCode ELSE '' END AS CYCLE_ID
,CASE WHEN extension.value:url = 'peiIdEffectivePeriod' THEN extension.value:valuePeriod:start WHEN extension_extension.value:url = 'peiIdEffectivePeriod' THEN extension_extension.value:valuePeriod:start ELSE '' END AS PEI_EFF_PERIOD_START
,CASE WHEN extension.value:url = 'peiIdEffectivePeriod' THEN extension.value:valuePeriod:end WHEN extension_extension.value:url = 'peiIdEffectivePeriod' THEN extension_extension.value:valuePeriod:end ELSE '' END AS PEI_EFF_PERIOD_END
,entry.value:resource:code:coding[0]:code AS ADMIN_ACT
FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource:extension,outer=>true) extension
,lateral flatten(extension.value:valueCodeableConcept:coding,outer=>true) extension_valueCodeableConcept_coding
,lateral flatten(extension.value:extension,outer=>true) extension_extension

WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'ProviderEntityIdentifier')

GROUP BY 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
ADMIN_ACT
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED);
QUALIFY  ROW_NUMBER() OVER (PARTITION BY ID,RESOURCE_TYPE, ADMIN_ACT ORDER BY LAST_UPDATED DESC) = 1;

------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------BCBSA.PDT_FHIR_VBP_DIM----------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
DELETE FROM BCBSA.PDT_FHIR_VBP_DIM;
INSERT INTO BCBSA.PDT_FHIR_VBP_DIM(
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
ADMIN_ACT,
PLAN_CD,
VBP_ID,
VBP_PROGRAM_TYPE_CD,
VBP_SCOPE_CD,
VBP_PROGRAM_NAME,
VBP_EFF_START,
VBP_EFF_END,
DE_DESIGNATION_DT,
TOTAL_CARE_EFF_START,
TOTAL_CARE_EFF_END,
LOAD_DT
)
SELECT 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
ADMIN_ACT,
MAX(PLAN_CD) AS PLAN_CD,
MAX(VBP_ID) AS VBP_ID,
MAX(VBP_PROGRAM_TYPE_CD) AS VBP_PROGRAM_TYPE_CD,
MAX(VBP_SCOPE_CD) AS VBP_SCOPE_CD,
MAX(VBP_PROGRAM_NAME) AS VBP_PROGRAM_NAME,
TRY_TO_DATE(MAX(VBP_EFF_START)) AS VBP_EFF_START,
TRY_TO_DATE(MAX(VBP_EFF_END)) AS VBP_EFF_END,
TRY_TO_DATE(MAX(DE_DESIGNATION_DT)) AS DE_DESIGNATION_DT,
TRY_TO_DATE(MAX(TOTAL_CARE_EFF_START)) AS TOTAL_CARE_EFF_START,
TRY_TO_DATE(MAX(TOTAL_CARE_EFF_END)) AS TOTAL_CARE_EFF_END,
CURRENT_DATE AS LOAD_DT
FROM (

SELECT 
entry.value:resource.id AS ID
,entry.value:resource:meta:lastUpdated AS LAST_UPDATED
,entry.value:resource:meta:versionId AS VERSION_ID
,entry.value:resource:resourceType AS RESOURCE_TYPE
,CASE WHEN extension.value:url = 'planCode' THEN extension.value:valueCode                      WHEN extension_extension.value:url = 'planCode' THEN extension_extension.value:valueCode ELSE '' END AS PLAN_CD
,CASE WHEN extension.value:url = 'vbpId' THEN extension.value:valueCode                         WHEN extension_extension.value:url = 'vbpId' THEN extension_extension.value:valueCode ELSE '' END AS VBP_ID
,CASE WHEN extension.value:url = 'vbpProgramTypeCode' THEN extension.value:valueCode            WHEN extension_extension.value:url = 'vbpProgramTypeCode' THEN extension_extension.value:valueCode ELSE '' END AS VBP_PROGRAM_TYPE_CD
,CASE WHEN extension.value:url = 'vbpScopeCode' THEN extension.value:valueCode                  WHEN extension_extension.value:url = 'vbpScopeCode' THEN extension_extension.value:valueCode ELSE '' END AS VBP_SCOPE_CD
,CASE WHEN extension.value:url = 'vbpProgramName' THEN extension.value:valueCode                WHEN extension_extension.value:url = 'vbpProgramName' THEN extension_extension.value:valueCode ELSE '' END AS VBP_PROGRAM_NAME
,CASE WHEN extension.value:url = 'vbpEffectivePeriod' THEN extension.value:valuePeriod:start    WHEN extension_extension.value:url = 'vbpEffectivePeriod' THEN extension_extension.value:valuePeriod:start ELSE '' END AS VBP_EFF_START
,CASE WHEN extension.value:url = 'vbpEffectivePeriod' THEN extension.value:valuePeriod:end      WHEN extension_extension.value:url = 'vbpEffectivePeriod' THEN extension_extension.value:valuePeriod:end ELSE '' END AS VBP_EFF_END
,CASE WHEN extension.value:url = 'deDesignationDate' THEN extension.value:valueDate             WHEN extension_extension.value:url = 'deDesignationDate' THEN extension_extension.value:valueDate ELSE '' END AS DE_DESIGNATION_DT
,CASE WHEN extension.value:url = 'totalCareEffectivePeriod' THEN extension.value:valuePeriod:start WHEN extension_extension.value:url = 'totalCareEffectivePeriod' THEN extension_extension.value:valuePeriod:start ELSE '' END AS TOTAL_CARE_EFF_START
,CASE WHEN extension.value:url = 'totalCareEffectivePeriod' THEN extension.value:valuePeriod:end WHEN extension_extension.value:url = 'totalCareEffectivePeriod' THEN extension_extension.value:valuePeriod:end ELSE '' END AS TOTAL_CARE_EFF_END
,entry.value:resource:code:coding[0]:code AS ADMIN_ACT
,extension_extension.value AS extension_extension

FROM BCBSA.PDT_FHIR A 
,lateral flatten(input=>FILE_CONTENT_RAW:entry,outer=>true) entry
,lateral flatten(entry.value:resource:extension,outer=>true) extension
,lateral flatten(extension.value:extension,outer=>true) extension_extension
WHERE FILE_RESOURCE_TYPE = 'Bundle' AND 
SPLIT_PART(FILE_NAME,'/',5)= 'ValueBasedProgramIdentifier'

)

GROUP BY 
ID,
LAST_UPDATED,
VERSION_ID,
RESOURCE_TYPE,
ADMIN_ACT
--HAVING  LAST_UPDATED = MAX(LAST_UPDATED);
QUALIFY  ROW_NUMBER() OVER (PARTITION BY ID,RESOURCE_TYPE, ADMIN_ACT ORDER BY LAST_UPDATED DESC) = 1;



   END;

ALTER TASK {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_REFERENCE_UPDATE_DAILY_TASK RESUME;
