CREATE OR REPLACE TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOCATION_LOAD_DAILY_TASK
WAREHOUSE = {{env}}_ELT_SML_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}_RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_LOCATION_TEMP LIKE  PDT_FHIR_LOCATION;


--DemographicExport
--TerminatingProviderExport
--QualityExport
--AnalyticsExportPII
--AnalyticsExport
INSERT INTO PDT_FHIR_LOCATION_TEMP(
	
      FILE_EXPORT_TYPE
     ,FILE_RECORD_ID
     ,VERSION_ID
     ,LAST_UPDTD_TS
     ,PROVIDER_LOCATION_MASTER_IDENTIFIER
     ,PROV_DBA_NAME
     ,PROV_ADDR_CITY
     ,PROV_ADDR_DISTRICT
     ,PROV_ADDR_STATE
     ,PROV_ADDR_POSTALCD
     ,PROV_ADDR_COUNTRY
     ,PROV_ADDR_USE
     ,PROV_ADDR_LINE1
     ,PROV_ADDR_LINE2
     ,PROV_COMMUNICATION_INFO_TYPE_PHONE
     ,PROV_COMMUNICATION_INFO_PHONE
     ,PROV_COMMUNICATION_INFO_PURPOSE_PHONE
     ,PROV_COMMUNICATION_INFO_TYPE_FAX
     ,PROV_COMMUNICATION_INFO_FAX
     ,PROV_COMMUNICATION_INFO_PURPOSE_FAX
     ,PROV_COMMUNICATION_INFO_TYPE_URL
     ,PROV_COMMUNICATION_INFO_URL
     ,PROV_COMMUNICATION_INFO_PURPOSE_URL
     ,DAYS_OF_WEEK_AND_OFFICE_HOURS
     ,POSITION_LATITUDE
     ,POSITION_LONGITUDE
     ,PROV_STAFF_LANGUAGE
     ,PROV_TERMINATION_CD
     ,SERVICE_AREA_CD
     ,GEOCODING_RETURN_CD
     ,METROPOLITAN_STATISTICAL_AREA
     ,PROFILE_TYPE
    ,LOAD_DT  
        )

WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='Location' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))

SELECT 
FILE_EXPORT_TYPE,FILE_RECORD_ID,
MAX(VERSION_ID) AS VERSION_ID,
MAX(LAST_UPDTD) AS LAST_UPDTD_TS,
MAX(Provider_Location_Master_Identifier) AS Provider_Location_Master_Identifier,
MAX(Provider_DBA_Name) AS PROV_DBA_NAME ,
MAX(Provider_Address_City) AS PROV_ADDR_CITY,
MAX(Provider_Address_District) AS PROV_ADDR_DISTRICT,
MAX(Provider_Address_State) AS PROV_ADDR_STATE,
MAX(Provider_Address_Postalcode) AS PROV_ADDR_POSTALCD,
MAX(Provider_Address_Country) AS PROV_ADDR_COUNTRY,
MAX(Provider_Address_Use) AS PROV_ADDR_USE,
MAX(Provider_Address_Line1) AS PROV_ADDR_LINE1,
MAX(Provider_Address_Line2) AS PROV_ADDR_LINE2 ,
MAX(Provider_Communication_Information_Type_phone) AS PROV_COMMUNICATION_INFO_TYPE_PHONE,
MAX(Provider_Communication_Information_phone) AS PROV_COMMUNICATION_INFO_PHONE,
MAX(Provider_Communication_Information_Purpose_phone) AS PROV_COMMUNICATION_INFO_PURPOSE_PHONE,
MAX(Provider_Communication_Information_Type_fax) AS PROV_COMMUNICATION_INFO_TYPE_FAX,
MAX(Provider_Communication_Information_fax) AS PROV_COMMUNICATION_INFO_FAX   , 
MAX(Provider_Communication_Information_Purpose_fax) AS PROV_COMMUNICATION_INFO_PURPOSE_FAX   , 
MAX(Provider_Communication_Information_Type_url) AS PROV_COMMUNICATION_INFO_TYPE_URL    ,
MAX(Provider_Communication_Information_url)    AS PROV_COMMUNICATION_INFO_URL,
MAX(Provider_Communication_Information_Purpose_url) AS PROV_COMMUNICATION_INFO_PURPOSE_URL,
MAX(DAYS_OF_WEEK_AND_OFFICE_HOURS)     AS DAYS_OF_WEEK_AND_OFFICE_HOURS,
MAX(Position_latitude) AS POSITION_LATITUDE    ,
MAX(Position_longitude) AS POSITION_LONGITUDE,
--MAX(Provider_Staff_Language) AS PROV_STAFF_LANGUAGE   , 
REGEXP_REPLACE(TRIM(LISTAGG(DISTINCT NVL(Provider_Staff_Language,''), '\n')),' +','') AS PROV_STAFF_LANGUAGE,
MAX(Provider_Termination_Code) AS PROV_TERMINATION_CD    ,
MAX(Service_Area_Code) AS SERVICE_AREA_CD    ,
MAX(Geocoding_Return_Code) AS GEOCODING_RETURN_CD,
MAX(Metropolitan_Statistical_Area) AS METROPOLITAN_STATISTICAL_AREA ,
MAX(PROFILE_TYPE) AS PROFILE_TYPE,  
LOAD_DT
FROM (
SELECT 
SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE,
A.FILE_RECORD_ID ,  
FILE_CONTENT_RAW:meta:versionId AS VERSION_ID,
FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD,
FILE_CONTENT_RAW:name AS Provider_DBA_Name,
FILE_CONTENT_RAW:description AS  Location_Description,
FILE_CONTENT_RAW:address:type as Provider_Address_Type,
  
FILE_CONTENT_RAW:address:city as Provider_Address_City,
FILE_CONTENT_RAW:address:district as Provider_Address_District,
FILE_CONTENT_RAW:address:state as Provider_Address_State,
FILE_CONTENT_RAW:address:postalCode as Provider_Address_Postalcode,
FILE_CONTENT_RAW:address:country as Provider_Address_Country,
FILE_CONTENT_RAW:address:use as Provider_Address_Use,
FILE_CONTENT_RAW:address:line[0] as Provider_Address_Line1,
FILE_CONTENT_RAW:address:line[1] as Provider_Address_Line2,

CASE WHEN (identifier_type_coding.value:code = 'PLM') THEN identifier.value:value else '' END AS Provider_Location_Master_Identifier,

CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_phone,
CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:value else ' ' END as Provider_Communication_Information_phone,
CASE WHEN (telecom.value:system = 'phone') THEN telecom.value:use else ' ' END  as  Provider_Communication_Information_Purpose_phone,

CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_fax,
CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:value else ' ' END as Provider_Communication_Information_fax,
CASE WHEN (telecom.value:system = 'fax') THEN telecom.value:use else ' ' END  as Provider_Communication_Information_Purpose_fax,
   
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:system else ' ' END as Provider_Communication_Information_Type_url,
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:value else ' ' END  as Provider_Communication_Information_url,
CASE WHEN (telecom.value:system = 'url') THEN telecom.value:use else ' ' END  as Provider_Communication_Information_Purpose_url,

  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[0] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[0]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[0]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[0]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[1] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[1]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[1]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[1]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[2] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[2]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[2]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[2]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[3] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[3]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[3]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[3]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[4] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[4]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[4]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[4]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[5] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[5]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[5]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[5]:closingTime ELSE ' ' END  ||'\n'||
  CASE WHEN (FILE_CONTENT_RAW:hoursOfOperation[6] IS NOT NULL) THEN FILE_CONTENT_RAW:hoursOfOperation[6]:daysOfWeek[0]||' '|| FILE_CONTENT_RAW:hoursOfOperation[6]:openingTime||'-'|| FILE_CONTENT_RAW:hoursOfOperation[6]:closingTime ELSE ' ' END  
  AS DAYS_OF_WEEK_AND_OFFICE_HOURS,


CASE WHEN (SPLIT_PART(extension.value:url,'/',-1)  = 'officelanguage' ) THEN extension_valueCodeableConcept_coding.value:code||':'|| NVL(extension_valueCodeableConcept_coding.value:display,'') END  AS Provider_Staff_Language,
CASE WHEN (SPLIT_PART(extension.value:url,'/',-1)  = 'provterm' ) THEN extension_valueCodeableConcept_coding.value:code||':'|| NVL(extension_valueCodeableConcept_coding.value:display,'') END  AS Provider_Termination_Code,
CASE WHEN (LOWER(SPLIT_PART(extension.value:url,'/',-1))  = 'servicearea' ) THEN extension_valueCodeableConcept_coding.value:code||':'|| NVL(extension_valueCodeableConcept_coding.value:display,'') END  AS Service_Area_Code,
CASE WHEN (LOWER(SPLIT_PART(extension.value:url,'/',-1))  = 'geocodereturn' ) THEN extension_valueCodeableConcept_coding.value:code||':'|| NVL(extension_valueCodeableConcept_coding.value:display,'') END  AS Geocoding_Return_Code,
CASE WHEN (LOWER(SPLIT_PART(extension.value:url,'/',-1))  = 'metroarea' ) THEN extension_valueCodeableConcept_coding.value:code||':'|| NVL(extension_valueCodeableConcept_coding.value:display,'') END  AS Metropolitan_Statistical_Area,
  
      
FILE_CONTENT_RAW:position:latitude as Position_latitude,
FILE_CONTENT_RAW:position:longitude as Position_longitude,
split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE,
LOAD_DT

FROM "BCBSA"."PDT_FHIR" A ,MAX_PDT_REC B,
lateral flatten(input=>FILE_CONTENT_RAW:telecom,outer=>true) telecom,
lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension,
lateral flatten(input=>extension.value:extension,outer=>true) extension_extension,
lateral flatten(extension.value:valueCodeableConcept:coding,outer=>true) extension_valueCodeableConcept_coding,
lateral flatten(extension_extension.value:valueCodeableConcept:coding,outer=>true) extension_extension_valueCodeableConcept_coding
,lateral flatten(input=>FILE_CONTENT_RAW:identifier,outer=>true) identifier
,lateral flatten(input=>identifier.value:type:coding,outer=>true) identifier_type_coding

WHERE FILE_RESOURCE_TYPE='Location' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND LOAD_DT IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_LOCATION )
)

GROUP BY FILE_EXPORT_TYPE,
FILE_RECORD_ID,LOAD_DT ;


MERGE INTO PDT_FHIR_LOCATION PR
USING PDT_FHIR_LOCATION_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_LOCATION  (SELECT * FROM PDT_FHIR_LOCATION_TEMP);

   END;

ALTER TASK {{env}}_RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOCATION_LOAD_DAILY_TASK RESUME;
