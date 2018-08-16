/***
Title:              step2_readmit_labels
Description:        Compute whether is a 30 d, 90 d readmit and whether is followed by a readmit at 30 d, 90 d. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step1_inpatient_cases
Output:             NATHALIE.PRJREA_STEP2_READMIT_LABELS
***/

/*
Compute "is_a_30d_readmit" and "is_a_90d_readmit".
This label is used when computing readmission rates at readmitting facilities, PPG, and other long term care facilities.
*/

drop table if exists nathalie.tmp
;

create table nathalie.tmp as
select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
from nathalie.prjrea_step1_inpatient_cases
;

refresh nathalie.tmp
;

insert into NATHALIE.tmp (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0)
; 

refresh nathalie.tmp
;

drop table if exists nathalie.tmp2
;

create table nathalie.tmp2 as
select *, row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber 
from nathalie.prjrea_step1_inpatient_cases
;


drop table if exists NATHALIE.tmp_base
;

create table NATHALIE.tmp_base 
as
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
    	  B.*
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
    FROM NATHALIE.tmp AS A LEFT JOIN nathalie.tmp2 AS B ON A.rownumber = B.rownumber - 1 
) AS S
;

refresh nathalie.tmp_base
;

/*
Compute "is_followed_by_a_30d_readmit" and "is_followed_by_a_90d_readmit".
This label is used as the outcome label when computing readmission rates at index facilities and when training a readmission prediction model.
*/

drop table if exists NATHALIE.tmp2;

create table NATHALIE.tmp2 as
select * from NATHALIE.tmp_base;

refresh nathalie.tmp2
;

insert into NATHALIE.tmp2 (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0);

refresh nathalie.tmp2
;

--main computation
drop table if exists NATHALIE.PRJREA_STEP2_READMIT_LABELS
;

create table NATHALIE.PRJREA_STEP2_READMIT_LABELS as
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
    	  A.*
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
    FROM NATHALIE.tmp2 AS A LEFT JOIN NATHALIE.tmp_base AS B ON A.rownumber = B.rownumber - 1 
) AS S
where case_id is not null
;

/*
Clean up
*/

drop table if exists tmp;
drop table if exists tmp2;
drop table if exists tmp_base;

select count(*) from PRJREA_STEP2_READMIT_LABELS where case_id is null;