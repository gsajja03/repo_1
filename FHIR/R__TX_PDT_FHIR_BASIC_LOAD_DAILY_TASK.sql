CREATE OR REPLACE TASK {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_BASIC_LOAD_DAILY_TASK
WAREHOUSE = {{env}}ELT_BIG_WH
--SCHEDULE  = 'USING CRON 0 2 * * * America/Chicago'
AFTER  {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_LOAD_DAILY_TASK
AS 

  BEGIN 
   
   USE DATABASE {{env}}RAW_INBOUND_DB;
   USE SCHEMA BCBSA; 

   CREATE OR REPLACE TEMPORARY TABLE PDT_FHIR_BASIC_TEMP LIKE  PDT_FHIR_BASIC;


INSERT INTO PDT_FHIR_BASIC_TEMP (
	
           FILE_EXPORT_TYPE
          ,FILE_RECORD_ID
          ,VERSION_ID
          ,LAST_UPDTD_TS
          ,LICENSEE_REF_RECORD_ID
          ,QUALITY_RECOGNITION_TYPE_CD
          ,QUALITY_RECOGNITION_TYPE_DISPLAY
          ,RECOGNITION_TYPE
          ,RECOGNITION_VALUE
          ,RECOGNITION_ENTITY
          ,RECOGNITION_WEBADDR
          ,RECOGNITION_PERIOD
          ,MASTER_ID
          ,MASTER_ID_TYPE
          ,HOSPITAL_OWNER
          ,HOSPITAL_TYPE
          ,EMERGENCY_SERVICE
          ,DECILE
          ,SURVEY_RESPONSE_RATE_PERCENT
          ,HCAHPS_ANSWER_PERCENT
          ,NUMBER_OF_SURVEYS
          ,HCAHPS_MEASURE_CD
          ,STAR_RATING
          ,LINEAR_MEAN
          ,MEASURE_CD
          ,HOSPITAL_SURVEY_RESPONSE_RATE_FOOTNOTE
          ,ZSCORE
          ,PROCESS_MEASURE_SAMPLE
          ,OBSERVED_RATE_OF_OCCURRENCES
          ,CONFIDENCE_INTERVAL_VALUE_LOW
          ,CONFIDENCE_INTERVAL_VALUE_HIGH
          ,ALT_RATING
          ,AT_RISK_PATIENTS
          ,OBSERVED_CASES
          ,EXPECTED_CASES
          ,QUINTILE_VALUE
          ,MESSAGING
          ,RISK_ADJUSTED_RATE
          ,SEVERITY_INDEX
          ,NUMBER_OF_HOSPITALS_AT_ALT_RATING_BENCHMARK
          ,STATE_LOW_CUTPOINT
          ,STATE_MEDIUM_CUTPOINT
          ,STATE_HIGH_CUTPOINT
          ,STATE_VERY_HIGH_CUTPOINT
          ,STATE_OVERALL_CUTPOINT
          ,STATE_AVERAGE
          ,STATE_AVERAGE_BOTTOM_20_PERCENT
          ,STATE_AVERAGE_TOP_20_PERCENT
          ,NATIONAL_LOW_CUTPOINT
          ,NATIONAL_MEDIUM_CUTPOINT
          ,NATIONAL_HIGH_CUTPOINT
          ,NATIONAL_VERY_HIGH_CUTPOINT
          ,NATIONAL_OVERALL_CUTPOINT
          ,NATIONAL_AVERAGE
          ,NATIONAL_AVERAGE_BOTTOM_20_PERCENT
          ,NATIONAL_AVERAGE_TOP20_PERCENT
          ,DIFFERENCE_IN_OBSERVED_AND_EXPECTED_RATES
          ,ACHIEVEMENT_POINTS_NUMERATOR
          ,ACHIEVEMENT_POINTS_DENOMINATOR
          ,IMPROVEMENT_POINTS_NUMERATOR
          ,IMPROVEMENT_POINTS_DENOMINATOR
          ,CONDITION_PROCEDURES_CORE
          ,MEASURE_DIM_SCORE_NUMERATOR
          ,MEASURE_DIM_SCORE_DENOMINATOR
          ,WEIGHTED_PERFORMANCE_SCORE
          ,UNWEIGHTED_PERFORMANCE_SCORE
          ,SCORE_DESIGNATION
          ,PROCESS_MEASURE_SCORE
          ,EMBOLD_HEALTH_MARKET
          ,EMBOLD_HEALTH_SPECIALTY
          ,EMBOLD_RECOGNITION_STATUS
          ,EMBOLD_HEALTH_TIER
          ,EMBOLD_HEALTH_ACCOUNT
          ,PROFILE_TYPE
          ,LOAD_DT
        )


WITH MAX_PDT_REC AS (SELECT FILE_RECORD_ID , SPLIT_PART(FILE_NAME,'/',3)  AS FILE_EXPORT_TYPE, MAX(FILE_LAST_MODIFIED) AS FILE_LAST_MODIFIED  FROM PDT_FHIR 
WHERE  FILE_RESOURCE_TYPE='Basic' 
GROUP BY FILE_RECORD_ID, SPLIT_PART(FILE_NAME,'/',3))

SELECT 
 FILE_EXPORT_TYPE
,FILE_RECORD_ID 
,MAX(VERSION_ID) AS VERSION_ID
,MAX(LAST_UPDTD) AS LAST_UPDTD_TS
,MAX(licensee_referece) AS LICENSEE_REF_RECORD_ID
,MAX(QualityRecognitionType_Code) AS QUALITY_RECOGNITION_TYPE_CD
,MAx(QualityRecognitionType_Display) AS QUALITY_RECOGNITION_TYPE_DISPLAY
,MAX(recognitionType) AS RECOGNITION_TYPE
,MAX(recognitionValue) AS RECOGNITION_VALUE
,MAX(recognitionEntity) AS RECOGNITION_ENTITY
,MAX(recognitionWebAddress) AS RECOGNITION_WEBADDR
,MAX(recognitionPeriod) AS RECOGNITION_PERIOD
,MAX(MasterIdentifier) AS MASTER_ID
,MAX(MasterIdentifier_Type) AS MASTER_ID_TYPE


,MAX( hospitalOwner                          )    AS   HOSPITAL_OWNER
,MAX( hospitalType                           )    AS   HOSPITAL_TYPE
,MAX( emergencyService                       )    AS   EMERGENCY_SERVICE
,MAX( decile                                 )    AS   DECILE
,MAX( surveyResponseRatePercent              )    AS   SURVEY_RESPONSE_RATE_PERCENT
,MAX(hcahpsAnswerPercent)                         AS   HCAHPS_ANSWER_PERCENT
,MAX( numberOfSurveys                        )    AS   NUMBER_OF_SURVEYS
,MAX( hcahpsMeasureCode                      )    AS   HCAHPS_MEASURE_CD
,MAX( starRating                             )    AS   STAR_RATING
,MAX( linearMean                             )    AS   LINEAR_MEAN


,MAX(measureCode)                                 AS   MEASURE_CD
,MAX(hospitalSurveyResponseRateFootnote)          AS   HOSPITAL_SURVEY_RESPONSE_RATE_FOOTNOTE
,MAX( zScore                                 )    AS   ZSCORE
,MAX( processMeasureSample                   )    AS   PROCESS_MEASURE_SAMPLE
,MAX( observedRateOfOccurrences              )    AS   OBSERVED_RATE_OF_OCCURRENCES
,MAX( confidenceIntervalValueLow             )    AS   CONFIDENCE_INTERVAL_VALUE_LOW
,MAX( confidenceIntervalValueHigh            )    AS   CONFIDENCE_INTERVAL_VALUE_HIGH
,MAX( alternativeRating                      )    AS   ALT_RATING
,MAX( atRiskPatients                         )    AS   AT_RISK_PATIENTS
,MAX( observedCases                          )    AS   OBSERVED_CASES
,MAX( expectedCases                          )    AS   EXPECTED_CASES
,MAX( quintileValue                          )    AS   QUINTILE_VALUE
,MAX( messaging                              )    AS   MESSAGING
,MAX( riskAdjustedRate                       )    AS   RISK_ADJUSTED_RATE
,MAX( severityIndex                          )    AS   SEVERITY_INDEX
,MAX( numberOfHospitalsAtAlternativeRatingBenchmark)      AS   NUMBER_OF_HOSPITALS_AT_ALT_RATING_BENCHMARK
,MAX( stateLowCutpoint                       )    AS   STATE_LOW_CUTPOINT
,MAX( stateMediumCutpoint                    )    AS   STATE_MEDIUM_CUTPOINT
,MAX( stateHighCutpoint                      )    AS   STATE_HIGH_CUTPOINT
,MAX( stateVeryHighCutpoint                  )    AS   STATE_VERY_HIGH_CUTPOINT
,MAX( stateOverallCutpoint                   )    AS   STATE_OVERALL_CUTPOINT
,MAX( stateAverage                           )    AS   STATE_AVERAGE
,MAX( stateAverageBottom20Percent            )    AS   STATE_AVERAGE_BOTTOM_20_PERCENT
,MAX( stateAverageTop20Percent               )    AS   STATE_AVERAGE_TOP_20_PERCENT
,MAX( nationalLowCutpoint                    )    AS   NATIONAL_LOW_CUTPOINT
,MAX( nationalMediumCutpoint                 )    AS   NATIONAL_MEDIUM_CUTPOINT
,MAX( nationalHighCutpoint                   )    AS   NATIONAL_HIGH_CUTPOINT
,MAX( nationalVeryHighCutpoint               )    AS   NATIONAL_VERY_HIGH_CUTPOINT
,MAX( nationalOverallCutpoint                )    AS   NATIONAL_OVERALL_CUTPOINT
,MAX( nationalAverage                        )    AS   NATIONAL_AVERAGE
,MAX( nationalAverageBottom20Percent         )    AS   NATIONAL_AVERAGE_BOTTOM_20_PERCENT
,MAX( nationalAverageTop20Percent            )    AS   NATIONAL_AVERAGE_TOP20_PERCENT
,MAX( differenceInObservedAndExpectedRates   )    AS   DIFFERENCE_IN_OBSERVED_AND_EXPECTED_RATES
,MAX( achievementPointsNumerator             )    AS   ACHIEVEMENT_POINTS_NUMERATOR
,MAX( achievementPointsDenominator           )    AS   ACHIEVEMENT_POINTS_DENOMINATOR
,MAX( improvementPointsNumerator             )    AS   IMPROVEMENT_POINTS_NUMERATOR
,MAX( improvementPointsDenominator           )    AS   IMPROVEMENT_POINTS_DENOMINATOR
,MAX( conditionProcedureScore                )    AS   CONDITION_PROCEDURES_CORE
,MAX( measureDimensionScoreNumerator         )    AS   MEASURE_DIM_SCORE_NUMERATOR
,MAX( measureDimensionScoreDenominator       )    AS   MEASURE_DIM_SCORE_DENOMINATOR
,MAX( weightedPerformanceScore               )    AS   WEIGHTED_PERFORMANCE_SCORE
,MAX( unweightedPerformanceScore             )    AS   UNWEIGHTED_PERFORMANCE_SCORE
,MAX( scoreDesignation                       )    AS   SCORE_DESIGNATION
,MAX( processMeasureScore                    )    AS   PROCESS_MEASURE_SCORE
,MAX( emboldMarket                           )    AS   EMBOLD_MKT
,MAX( emboldSpecialty                        )    AS   EMBOLD_SPECIALTY
,MAX( emboldStatus                           )    AS   EMBOLD_STATUS
,MAX( emboldTier                             )    AS   EMBOLD_TIER
,MAX( emboldAccount                          )    AS   EMBOLD_ACCNT
,MAX(PROFILE_TYPE) AS PROFILE_TYPE
,LOAD_DT


FROM 
(

 SELECT
 SPLIT_PART(FILE_NAME,'/',3) AS FILE_EXPORT_TYPE
,A.FILE_RECORD_ID 
,FILE_CONTENT_RAW:meta:versionId AS VERSION_ID
,FILE_CONTENT_RAW:meta:lastUpdated AS LAST_UPDTD
,extension.value AS extension
,case when (split_part(code_coding.value:system,'/',-1) = 'QualityRecognitionTypeCS') then code_coding.value:code else '' end as QualityRecognitionType_Code 
,case when (split_part(code_coding.value:system,'/',-1) = 'QualityRecognitionTypeCS') then code_coding.value:display else '' end as QualityRecognitionType_Display 
,case when (split_part(extension_extension.value:url,'/',-1) = 'licensee') then extension_extension.value:valueReference:reference else '' end as Licensee_Reference 
,case when (split_part(extension_extension.value:url,'/',-1) = 'recognitionType') then extension_extension_valueCodeableConcept_coding.value:code else '' end as recognitionType 
,case when (split_part(extension_extension.value:url,'/',-1) = 'recognitionValue') then extension_extension_valueCodeableConcept_coding.value:code else '' end as recognitionValue 
,case when (split_part(extension_extension.value:url,'/',-1) = 'recognizingEntity') then extension_extension_valueCodeableConcept_coding.value:code else '' end as recognitionEntity 
,case when (split_part(extension_extension.value:url,'/',-1) = 'recognitionWebAddress') then extension_extension.value:valueUrl else '' end as recognitionWebAddress 
,case when (split_part(extension_extension.value:url,'/',-1) = 'recognitionPeriod') then NVL(extension_extension.value:valuePeriod:start,'')||':'|| NVL(extension_extension.value:valuePeriod:end,'') else '' end as recognitionPeriod 
--,case when (split_part(identifier.value:system,'/',-1) = 'MasterIdentifier') then  identifier.value:value else '' end as MasterIdentifier
,identifier.value:value  as MasterIdentifier 
--,case when (split_part(identifier_type_coding.value:code,'/',-1) = 'PLM') then identifier_type_coding.value:code else '' end as MasterIdentifier_Type 
,identifier_type_coding.value:code  as MasterIdentifier_Type

,case when (split_part(extension_extension.value:url,'/',-1) = 'licensee')                              then split_part(extension_extension.value:valueReference:reference,'/',-1) else '' end as LICENSEE_REFERECE 
,case when (split_part(extension_extension.value:url,'/',-1) = 'hcahpsMeasureCode')                     then extension_extension.value:valueString              else '' end as hcahpsMeasureCode 
,case when (split_part(extension_extension.value:url,'/',-1) = 'measureCode')                           then extension_extension.value:valueString              else '' end as measureCode 
,case when (split_part(extension_extension.value:url,'/',-1) = 'decile')                                then extension_extension.value:valueInteger             else '' end as decile 
,case when (split_part(extension_extension.value:url,'/',-1) = 'hcahpsAnswerPercent')                   then extension_extension.value:valueString              else '' end as hcahpsAnswerPercent
,case when (split_part(extension_extension.value:url,'/',-1) = 'hospitalSurveyResponseRateFootnote')    then extension_extension.value:valueString              else '' end as hospitalSurveyResponseRateFootnote 
,case when (split_part(extension_extension.value:url,'/',-1) = 'surveyResponseRatePercent')             then extension_extension.value:valueString              else '' end as surveyResponseRatePercent 
,case when (split_part(extension_extension.value:url,'/',-1) = 'zScore')                                then extension_extension.value:valueDecimal             else '' end as zScore 
,case when (split_part(extension_extension.value:url,'/',-1) = 'observedRateOfOccurrences')             then extension_extension.value:valueInteger             else '' end as observedRateOfOccurrences 
,case when (split_part(extension_extension.value:url,'/',-1) = 'achievementPointsNumerator')            then extension_extension.value:valueInteger             else '' end as achievementPointsNumerator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'confidenceIntervalValueHigh')           then extension_extension.value:valueInteger             else '' end as confidenceIntervalValueHigh 
,case when (split_part(extension_extension.value:url,'/',-1) = 'processMeasureSample')                  then extension_extension.value:valueInteger             else '' end as processMeasureSample
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateOverallCutpoint')                  then extension_extension.value:valueInteger             else '' end as stateOverallCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'numberOfSurveys')                       then extension_extension.value:valueString              else '' end as numberOfSurveys 
,case when (split_part(extension_extension.value:url,'/',-1) = 'achievementPointsDenominator')          then extension_extension.value:valueInteger             else '' end as achievementPointsDenominator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'improvementPointsDenominator')          then extension_extension.value:valueInteger             else '' end as improvementPointsDenominator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'conditionProcedureScore')               then extension_extension.value:valueInteger             else '' end as conditionProcedureScore 
,case when (split_part(extension_extension.value:url,'/',-1) = 'alternativeRating')                     then extension_extension.value:valueString              else '' end as alternativeRating 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalVeryHighCutpoint')              then extension_extension.value:valueInteger             else '' end as nationalVeryHighCutpoint
,case when (split_part(extension_extension.value:url,'/',-1) = 'measureDimensionScoreNumerator')        then extension_extension.value:valueInteger             else '' end as measureDimensionScoreNumerator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'hospitalOwner')                         then extension_extension_valueCodeableConcept_coding.value:code       else '' end as hospitalOwner 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateAverageTop20Percent')              then extension_extension.value:valueInteger             else '' end as stateAverageTop20Percent 
,case when (split_part(extension_extension.value:url,'/',-1) = 'atRiskPatients')                        then extension_extension.value:valueInteger             else '' end as atRiskPatients 
,case when (split_part(extension_extension.value:url,'/',-1) = 'observedCases')                         then extension_extension.value:valueInteger             else '' end as observedCases 
,case when (split_part(extension_extension.value:url,'/',-1) = 'expectedCases')                         then extension_extension.value:valueInteger             else '' end as expectedCases 
,case when (split_part(extension_extension.value:url,'/',-1) = 'quintileValue')                         then extension_extension.value:valueInteger             else '' end as quintileValue
,case when (split_part(extension_extension.value:url,'/',-1) = 'hospitalType')                          then extension_extension_valueCodeableConcept_coding.value:code       else '' end as hospitalType 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateLowCutpoint')                      then extension_extension.value:valueInteger             else '' end as stateLowCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateHighCutpoint')                     then extension_extension.value:valueInteger             else '' end as  stateHighCutpoint
,case when (split_part(extension_extension.value:url,'/',-1) = 'improvementPointsNumerator')            then extension_extension.value:valueInteger             else '' end as improvementPointsNumerator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'emergencyService')                      then extension_extension_valueCodeableConcept_coding.value:code       else '' end as emergencyService
,case when (split_part(extension_extension.value:url,'/',-1) = 'numberOfHospitalsAtAlternativeRatingBenchmark') then extension_extension.value:valueString      else '' end as numberOfHospitalsAtAlternativeRatingBenchmark 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateMediumCutpoint')                   then extension_extension.value:valueInteger             else '' end as stateMediumCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalMediumCutpoint')                then extension_extension.value:valueInteger             else '' end as nationalMediumCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateVeryHighCutpoint')                 then extension_extension.value:valueInteger             else '' end as stateVeryHighCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalOverallCutpoint')               then extension_extension.value:valueInteger             else '' end as nationalOverallCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'messaging')                             then extension_extension.value:valueString              else '' end as messaging
,case when (split_part(extension_extension.value:url,'/',-1) = 'riskAdjustedRate')                      then extension_extension.value:valueInteger             else '' end as riskAdjustedRate 
,case when (split_part(extension_extension.value:url,'/',-1) = 'scoreDesignation')                      then extension_extension.value:valueString              else '' end as scoreDesignation 
,case when (split_part(extension_extension.value:url,'/',-1) = 'processMeasureScore')                   then extension_extension.value:valueInteger             else '' end as processMeasureScore 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalAverage')                       then extension_extension.value:valueInteger             else '' end as nationalAverage
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateAverageBottom20Percent')           then extension_extension.value:valueInteger             else '' end as stateAverageBottom20Percent
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalAverageBottom20Percent')        then extension_extension.value:valueInteger             else '' end as nationalAverageBottom20Percent
,case when (split_part(extension_extension.value:url,'/',-1) = 'measureDimensionScoreDenominator')      then extension_extension.value:valueInteger             else '' end as measureDimensionScoreDenominator 
,case when (split_part(extension_extension.value:url,'/',-1) = 'differenceInObservedAndExpectedRates')  then extension_extension.value:valueInteger             else '' end as differenceInObservedAndExpectedRates 
,case when (split_part(extension_extension.value:url,'/',-1) = 'stateAverage')                          then extension_extension.value:valueInteger             else '' end as stateAverage
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalLowCutpoint')                   then extension_extension.value:valueInteger             else '' end as nationalLowCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'weightedPerformanceScore')              then extension_extension.value:valueDecimal             else '' end as weightedPerformanceScore 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalHighCutpoint')                  then extension_extension.value:valueInteger             else '' end as nationalHighCutpoint 
,case when (split_part(extension_extension.value:url,'/',-1) = 'confidenceIntervalValueLow')            then extension_extension.value:valueInteger             else '' end as confidenceIntervalValueLow 
,case when (split_part(extension_extension.value:url,'/',-1) = 'unweightedPerformanceScore')            then extension_extension.value:valueDecimal             else '' end as unweightedPerformanceScore 
,case when (split_part(extension_extension.value:url,'/',-1) = 'nationalAverageTop20Percent')           then extension_extension.value:valueInteger             else '' end as nationalAverageTop20Percent
,case when (split_part(extension_extension.value:url,'/',-1) = 'severityIndex')                         then extension_extension.value:valueDecimal             else '' end as severityIndex  
,case when (split_part(extension_extension.value:url,'/',-1) = 'starRating')                            then extension_extension.value:valueInteger             else '' end as starRating  
,case when (split_part(extension_extension.value:url,'/',-1) = 'linearMean')                            then extension_extension.value:valueInteger             else '' end as linearMean  

,case when (split_part(extension_extension.value:url,'/',-1) = 'emboldMarket')                          then extension_extension.value:valueString             else '' end as emboldMarket  
,case when (split_part(extension_extension.value:url,'/',-1) = 'emboldSpecialty')                       then extension_extension.value:valueString             else '' end as emboldSpecialty  
,case when (split_part(extension_extension.value:url,'/',-1) = 'emboldStatus')                          then extension_extension.value:valueString             else '' end as emboldStatus  
,case when (split_part(extension_extension.value:url,'/',-1) = 'emboldAccount')                         then extension_extension.value:valueString             else '' end as emboldAccount  
,case when (split_part(extension_extension.value:url,'/',-1) = 'emboldTier')                            then extension_extension.value:valueString             else '' end as emboldTier  

,FILE_CONTENT_RAW:id     AS embold_id
,split_part(FILE_CONTENT_RAW:id,'-',-1) AS PROFILE_TYPE
,LOAD_DT  

FROM "BCBSA"."PDT_FHIR" A ,MAX_PDT_REC B
,lateral flatten(input=>FILE_CONTENT_RAW:code:coding,outer=>true) code_coding 
,lateral flatten(input=>FILE_CONTENT_RAW:extension,outer=>true) extension 
,lateral flatten(input=>extension.value:extension,outer=>true) extension_extension 
,lateral flatten(extension_extension.value:valueCodeableConcept:coding,outer=>true) extension_extension_valueCodeableConcept_coding 
,lateral flatten(input=>FILE_CONTENT_RAW:identifier,outer=>true) identifier 
,lateral flatten(input=>identifier.value:type:coding,outer=>true) identifier_type_coding 

WHERE FILE_RESOURCE_TYPE='Basic' 
AND A.FILE_RECORD_ID = B.FILE_RECORD_ID AND A.FILE_LAST_MODIFIED = B.FILE_LAST_MODIFIED AND SPLIT_PART(A.FILE_NAME,'/',3) =B.FILE_EXPORT_TYPE
AND A.LOAD_DT  IN ( SELECT DISTINCT LOAD_DT FROM PDT_FHIR MINUS  SELECT DISTINCT LOAD_DT FROM PDT_FHIR_BASIC ) 
) 
GROUP BY  FILE_EXPORT_TYPE
,FILE_RECORD_ID 
,extension,
LOAD_DT;



MERGE INTO PDT_FHIR_BASIC PR
USING PDT_FHIR_BASIC_TEMP PR_TEMP ON  TRIM(PR.FILE_RECORD_ID) = TRIM(PR_TEMP.FILE_RECORD_ID) AND TRIM(PR.FILE_EXPORT_TYPE) = TRIM(PR_TEMP.FILE_EXPORT_TYPE)
WHEN MATCHED THEN DELETE; 

INSERT INTO PDT_FHIR_BASIC  (SELECT * FROM PDT_FHIR_BASIC_TEMP);

   END;

ALTER TASK {{env}}RAW_INBOUND_DB.BCBSA.PDT_FHIR_BASIC_LOAD_DAILY_TASK RESUME;
