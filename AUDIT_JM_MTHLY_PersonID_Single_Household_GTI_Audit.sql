create or replace table `fxtl-staging-ds-7cde.ds_couchm_PERSONID.AUDIT_JM_MTHLY_PersonID_Single_Household_GTI` as (  

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

AGG_VIEWING_PER_PROGRAM AS (
  SELECT 
  REFERENCE_DATE,
  CONTENT_TYPE_CATEGORY2,
  TITLE_ID,
  CONTENT_TITLE,
  COUNT(DISTINCT(ACCOUNT_NUMBER)) AS total_accounts_pp,
  COUNT(DISTINCT(IF (GENDER='F', ACCOUNT_NUMBER, NULL))) AS female_accounts_pp,
  COUNT(DISTINCT(IF (GENDER='M', ACCOUNT_NUMBER, NULL))) AS male_accounts_pp,
  SUM(CASE WHEN GENDER='F' THEN VIEWING_DURATION_MIN ELSE 0 END) AS female_duration_pp,
  SUM(CASE WHEN GENDER='M' THEN VIEWING_DURATION_MIN ELSE 0 END) AS male_duration_pp,
  FROM MONTHLY_VIEWING_PER_PROGRAM
  GROUP BY 1,2,3,4
),

AGG_VIEWING_PER_CATEGORY AS (
  SELECT 
  REFERENCE_DATE,
  CONTENT_TYPE_CATEGORY2,
  COUNT(DISTINCT(ACCOUNT_NUMBER)) AS total_accounts,
  COUNT(DISTINCT(IF (GENDER='F', ACCOUNT_NUMBER, NULL))) AS female_accounts,
  COUNT(DISTINCT(IF (GENDER='M', ACCOUNT_NUMBER, NULL))) AS male_accounts,
  SUM(CASE WHEN GENDER='F' THEN VIEWING_DURATION_MIN ELSE 0 END) AS female_duration,
  SUM(CASE WHEN GENDER='M' THEN VIEWING_DURATION_MIN ELSE 0 END) AS male_duration,
  FROM MONTHLY_VIEWING_PER_PROGRAM
  GROUP BY 1, 2
),

# AGG_VIEWING_MONTH_TOTAL AS (
#   SELECT 
#   REFERENCE_DATE,
#   COUNT(DISTINCT(ACCOUNT_NUMBER)) AS total_accounts,
#   COUNT(DISTINCT(IF (GENDER='F', ACCOUNT_NUMBER, NULL))) AS female_accounts,
#   COUNT(DISTINCT(IF (GENDER='M', ACCOUNT_NUMBER, NULL))) AS male_accounts,
#   SUM(CASE WHEN GENDER='F' THEN VIEWING_DURATION_MIN ELSE 0 END) AS female_duration,
#   SUM(CASE WHEN GENDER='M' THEN VIEWING_DURATION_MIN ELSE 0 END) AS male_duration,
#   FROM MONTHLY_VIEWING_PER_PROGRAM
#   GROUP BY 1
# ),

PROGRAM_SUMMARY AS (
  SELECT
    a.REFERENCE_DATE,
    a.CONTENT_TYPE_CATEGORY2,
    a.TITLE_ID,
    a.CONTENT_TITLE,
    a.total_accounts_pp,
    a.female_accounts_pp,
    a.male_accounts_pp,
    a.female_duration_pp,
    a.male_duration_pp,
    b.total_accounts,
	  b.female_accounts,
	  b.male_accounts,
    b.female_duration,
    b.male_duration,
    b.female_duration + b.male_duration AS duration,
    a.female_duration_pp + a.male_duration_pp AS duration_pp,
    a.total_accounts_pp / b.total_accounts AS penetration,
    CASE WHEN (b.female_duration + b.male_duration) > 0 THEN b.female_duration/(b.female_duration + b.male_duration) ELSE -1 END AS female_duration_ratio,
    CASE WHEN (a.female_duration_pp + a.male_duration_pp) > 0 THEN a.female_duration_pp/(a.female_duration_pp + a.male_duration_pp) ELSE -1 END AS female_duration_pp_ratio,

  FROM AGG_VIEWING_PER_PROGRAM AS a
  LEFT JOIN AGG_VIEWING_PER_CATEGORY AS b
  USING (REFERENCE_DATE, CONTENT_TYPE_CATEGORY2)
  )


SELECT
    *,
    ROUND(
      CASE WHEN female_duration_ratio>0 
      THEN female_duration_pp_ratio/female_duration_ratio ELSE 1 END, 2) AS GTI_program,
  FROM PROGRAM_SUMMARY
  )
