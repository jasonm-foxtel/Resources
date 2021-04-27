create or replace table `fxtl-staging-ds-7cde.ds_couchm_PERSONID.AUDIT_JM_PIVOT_MTHLY_PersonID_Single_Household_GTI` as (

WITH BASE AS (
    SELECT DISTINCT
    REFERENCE_DATE,
    CONTENT_TYPE_CATEGORY2,
    TITLE_ID,
    CONTENT_TITLE
    FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.PersonID_Single_Household`
),

WIDE AS (  
    SELECT 
    REFERENCE_DATE,
    CONTENT_TYPE_CATEGORY2,
    TITLE_ID,
    CONTENT_TITLE,
	ACCOUNT_NUMBER,
    GENDER,
    VIEWING_DURATION_MIN
    FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.PersonID_Single_Household`
),

MONTHLY_VIEWING_PER_PROGRAM AS (
    SELECT 
    B.REFERENCE_DATE,
    B.CONTENT_TYPE_CATEGORY2,
    B.TITLE_ID,
    B.CONTENT_TITLE,
	A.ACCOUNT_NUMBER,
    A.GENDER,
    A.VIEWING_DURATION_MIN
     FROM BASE B
    LEFT JOIN WIDE A
    USING (CONTENT_TYPE_CATEGORY2, TITLE_ID, CONTENT_TITLE)
    WHERE A.REFERENCE_DATE between DATE_SUB(B.REFERENCE_DATE, INTERVAL 30 DAY) and B.REFERENCE_DATE
),

AGG_VIEWING_MONTH_TOTAL AS (
  SELECT 
  REFERENCE_DATE,
  COUNT(DISTINCT(ACCOUNT_NUMBER)) AS monthly_total_accounts,
  COUNT(DISTINCT(IF (GENDER='F', ACCOUNT_NUMBER, NULL))) AS monthly_total_female_accounts,
  COUNT(DISTINCT(IF (GENDER='M', ACCOUNT_NUMBER, NULL))) AS monthly_total_male_accounts,
  SUM(CASE WHEN GENDER='F' THEN VIEWING_DURATION_MIN ELSE 0 END) AS monthly_total_female_duration,
  SUM(CASE WHEN GENDER='M' THEN VIEWING_DURATION_MIN ELSE 0 END) AS monthly_total_male_duration,
  FROM MONTHLY_VIEWING_PER_PROGRAM
  GROUP BY 1
),

PIVOTED as (
  SELECT
    REFERENCE_DATE,
    TITLE_ID,
    CONTENT_TITLE,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'LIVE' , female_accounts_pp, 0 ) ) as numSingleFemaleAccountsLIVE,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'LIVE' , male_accounts_pp, 0 ) ) as numSingleMaleAccountsLIVE,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'LIVE' , female_duration_pp, 0 ) ), 2 ) as sumSingleFemaleDurationLIVE,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'LIVE' , male_duration_pp, 0 ) ), 2 ) as sumSingleMaleDurationLIVE,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'PLAYBACK' , female_accounts_pp, 0 ) ) as numSingleFemaleAccountsPLAYBACK,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'PLAYBACK' , male_accounts_pp, 0 ) ) as numSingleMaleAccountsPLAYBACK,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'PLAYBACK' , female_duration_pp, 0 ) ), 2 ) as sumSingleFemaleDurationPLAYBACK,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'PLAYBACK' , male_duration_pp, 0 ) ), 2 ) as sumSingleMaleDurationPLAYBACK,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'VOD' , female_accounts_pp, 0 ) ) as numSingleFemaleAccountsVOD,
    SUM( IF( CONTENT_TYPE_CATEGORY2 = 'VOD' , male_accounts_pp, 0 ) ) as numSingleMaleAccountsVOD,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'VOD' , female_duration_pp, 0 ) ), 2 ) as sumSingleFemaleDurationVOD,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'VOD' , male_duration_pp, 0 ) ), 2 ) as sumSingleMaleDurationVOD,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'LIVE' , duration, 0 ) ), 2 ) as sumGlobalDurationLIVE,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'PLAYBACK' , duration, 0 ) ), 2 ) as sumGlobalDurationPLAYBACK,
    ROUND( SUM( IF( CONTENT_TYPE_CATEGORY2 = 'VOD' , duration, 0 ) ), 2 ) as sumGlobalDurationVOD,
    ROUND(MAX(penetration), 4) as penetration
FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.AUDIT_JM_MTHLY_PersonID_Single_Household_GTI` 
GROUP BY 1,2,3 
)

SELECT P.*,
    AV.monthly_total_female_accounts as numSingleFemalesTOTAL,
    AV.monthly_total_male_accounts as numSingleMalesTOTAL,
    AV.monthly_total_accounts as numAccountsTOTAL
FROM PIVOTED P
LEFT JOIN AGG_VIEWING_MONTH_TOTAL AV
USING(REFERENCE_DATE)
)
ORDER BY penetration desc