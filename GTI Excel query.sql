/*
GTI_Excel_query.sql
This script is used to retrieve info for an ad-hoc GTI request.

The data was taken from Michael's ds_couchm_PERSONID dataset, with the REFERENCE_DATE set to 2020-01-01, as this was a known date with available data. 
The REFERENCE_DATE does not change in the output excel, but was kept to reflect this sourcing information.

The output is ordered by account_penetration descending.

Explanation of columns (to my understanding):
	TITLE_ID - Unique ID for a given program
	CONTENT_TITLE - Name of program
	GTI - The final Gender Tendency Index (ratio of female/male viewing) used by PersonID 
	PREDICTED_GTI_PROGRAM_FLAG - If the GTI is obtained from the Python prediction, this = 1.
								- If the GTI is obtained from single household viewership, this = 0.

-- The following columns were joined from the intermediate table 'PersonID_Single_Household_GTI':
	(These columns will be null or small values when PREDICTED_GTI_PROGRAM_FLAG = 1)

	single_hh_total_female_pp - Total distinct single household female viewers for this program
	single_hh_total_male_pp - Total distinct single household male viewers for this program
	single_hh_sum_female_view_duration_pp - Total number of hours of this program watched by single household female viewers
	single_hh_sum_male_view_duration_pp - Total number of hours of this program watched by single household male viewers

-- The following columns were joined from the intermediate table 'PersonID_Programs_Master':

	tot_distinct_accounts - Total number of distinct accounts which viewed the program
	avg_tot_duration_per_account - Average number of hours watched per distinct account
	sum_tot_duration - Total number of hours watched for all accounts (the product of the two above columns)
	account_penetration - Proportion of all accounts from the given viewing period (day?) which viewed this program

*/

with single_hh as (
  SELECT REFERENCE_DATE,
    TITLE_ID,
    CONTENT_TITLE,
    sum(total_female_pp) as single_hh_total_female_pp,	
    sum(total_male_pp) as single_hh_total_male_pp,
    round(sum(sum_female_view_duration_pp),2) as single_hh_sum_female_view_duration_pp,
    round(sum(sum_male_view_duration_pp),2) as single_hh_sum_male_view_duration_pp
  FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.PersonID_Single_Household_GTI` AS A 
  WHERE REFERENCE_DATE = DATE("2020-01-01")
  GROUP BY REFERENCE_DATE, TITLE_ID, CONTENT_TITLE
),

prog_master as (
SELECT REFERENCE_DATE,
    TITLE_ID,
    CONTENT_TITLE,
    number_of_accounts as tot_distinct_accounts,	
    round(avg_tot_duration_per_account, 4) as avg_tot_duration_per_account,
	round(avg_tot_duration_per_account*number_of_accounts, 4) as sum_tot_duration,
    account_penetration
  FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.PersonID_Programs_Master` AS A 
  WHERE REFERENCE_DATE = DATE("2020-01-01")
),

gti_prog as (
SELECT REFERENCE_DATE,
    TITLE_ID,
    CONTENT_TITLE,	
    round(GTI_PROGRAM,4) as GTI,	
    PREDICTED_GTI_PROGRAM_FLAG
  FROM `fxtl-staging-ds-7cde.ds_couchm_PERSONID.PersonID_GTI_Program` AS A 
  WHERE REFERENCE_DATE = DATE("2020-01-01")
  order by REFERENCE_DATE, TITLE_ID, CONTENT_TITLE    
)

select * from gti_prog
left join single_hh
using(REFERENCE_DATE, TITLE_ID, CONTENT_TITLE)
left join prog_master 
using(REFERENCE_DATE, TITLE_ID, CONTENT_TITLE)
order by account_penetration desc
  