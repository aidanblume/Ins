/***
Title:              step2_readmit_labels
Description:        Compute whether is a 30 d, 90 d readmit and whether is followed by a readmit at 30 d, 90 d. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step1_inpatient_cases
Output:             NATHALIE.PRJREA_STEP2_READMIT_LABELS
***/

select adm_dt, dis_dt
    , days_since_prior_discharge 
    , is_a_30d_readmit
    , is_a_90d_readmit
from PRJREA_STEP2_READMIT_LABELS -- 793021
-- where days_since_prior_discharge is null and (is_a_30d_readmit>0 or is_a_90d_readmit>0)
-- where is_a_30d_readmit>0 and is_a_90d_readmit=0
where days_since_prior_discharge is not null

drop table if exists NATHALIE.PRJREA_STEP2_READMIT_LABELS
;

create table NATHALIE.PRJREA_STEP2_READMIT_LABELS 
as
select Ca.*
    , LookBack.days_since_prior_discharge
    , LookBack.prior_stay_case_id
    , LookBack.prior_stay_LOS
    , LookBack.is_a_30d_readmit
    , LookBack.is_a_90d_readmit
    , LookForward.days_until_next_admit
    , LookForward.subsequent_stay_case_id
    , LookForward.subsequent_stay_los
    , LookForward.is_followed_by_a_30d_readmit
    , LookForward.is_followed_by_a_90d_readmit
from nathalie.prjrea_step1_inpatient_cases as Ca
left join
(
    /*
    Compute "is_a_30d_readmit" and "is_a_90d_readmit".
    This label is used when computing readmission rates at readmitting facilities, PPG, and other long term care facilities.
    */
    
    SELECT 
        *
        , case
            when days_since_prior_discharge <= 30 then 1 
            else 0 
          end as is_a_30d_readmit
        , case
            when days_since_prior_discharge <= 90 then 1 
            else 0 
          end as is_a_90d_readmit
    FROM
    (-- A is earlier than B; 1st row for A is nulls/ancient dates
    	SELECT 
        	  B.case_id
        	, CASE
            		WHEN A.cin_no = B.cin_no AND  DATEDIFF(B.adm_dt, A.dis_dt) >= 0 THEN DATEDIFF(B.adm_dt, A.dis_dt)
            		ELSE NULL
        	    END AS days_since_prior_discharge
        	, case
            	    when A.cin_no = B.cin_no  AND  DATEDIFF(B.adm_dt, A.dis_dt) >= 0 then A.case_id
            	    else null
            	end as prior_stay_case_id
        	, case
            	    when A.cin_no = B.cin_no  AND  DATEDIFF(B.adm_dt, A.dis_dt) >= 0 then A.LOS
            	    else null
            	end as prior_stay_LOS
        FROM 
        (
            select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
            from nathalie.prjrea_step1_inpatient_cases
            union
            select null, null, '1900-01-01', '1900-01-01', null, null, null, null, null, null, null, null, 0
        ) AS A 
        LEFT JOIN 
        (
            select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
            from nathalie.prjrea_step1_inpatient_cases
        ) AS B 
        ON A.rownumber = B.rownumber - 1 
    ) AS S
    where case_id is not null
) as LookBack
on Ca.case_id=LookBack.case_id
left join  
(
    /*
    Compute "is_followed_by_a_30d_readmit" and "is_followed_by_a_90d_readmit".
    This label is used as the outcome label when computing readmission rates at index facilities and when training a readmission prediction model.
    */

    SELECT 
        *
        , case
            when days_until_next_admit <= 30 then 1 
            else 0 
          end as is_followed_by_a_30d_readmit
        , case
            when days_until_next_admit <= 90 then 1 
            else 0 
          end as is_followed_by_a_90d_readmit
    FROM
    (-- A is earlier than B
    	SELECT 
        	  A.case_id
        	, CASE
            		WHEN A.cin_no = B.cin_no AND  DATEDIFF(B.adm_dt, A.dis_dt) >= 0 THEN DATEDIFF(B.adm_dt, A.dis_dt)
            		ELSE NULL
        	    END AS days_until_next_admit
        	, case
            	    when A.cin_no = B.cin_no then B.case_id
            	    else null
            	end as subsequent_stay_case_id
        	, case
            	    when A.cin_no = B.cin_no then B.LOS
            	    else null
            	end as subsequent_stay_LOS
        FROM 
        ( 
            select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
            from nathalie.prjrea_step1_inpatient_cases
            union
            select null, null, '1900-01-01', '1900-01-01', null, null, null, null, null, null, null, null, 0
        ) AS A 
        LEFT JOIN 
        (
            select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
            from nathalie.prjrea_step1_inpatient_cases
        ) AS B 
        ON A.rownumber = B.rownumber - 1 
    ) AS S
    where case_id is not null
) as LookForward
on Ca.case_id=LookForward.case_id
where Ca.case_id is not null
;
