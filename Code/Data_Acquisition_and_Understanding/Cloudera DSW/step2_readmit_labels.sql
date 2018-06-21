/***
Title:              step2_readmit_labels
Description:        Compute whether is a 30 d, 90 d readmit and whether is followed by a readmit at 30 d, 90 d. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step1_inpatient_cases
Output:             NATHALIE.PRJREA_STEP2_READMIT_LABELS
***/

/*
IS A 30 d, 90 d READMIT? For each case, is it preceded by an admission within 30 days or not? Label 'I' if no, and 'R' if yes. 

Create a copy of the input table called tmp. Join input table to copy with offset 1. Save values for latest admit per row, plus save small set of data concerning earliest admit if exists. 
To solve row shearing from join, add a dummy row to tmp with rownumber is 0, dis_dt/adm_dt 100 years in past, and other vals null.

*/

drop table if exists nathalie.tmp
;

create table nathalie.tmp as
select * from nathalie.prjrea_step1_inpatient_cases
;

insert into NATHALIE.tmp (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0)
; 

drop table if exists NATHALIE.njb_labeled_as_readmits
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
    FROM NATHALIE.tmp AS A LEFT JOIN nathalie.prjrea_step1_inpatient_cases AS B ON A.rownumber = B.rownumber - 1 
) AS S
;


/*
PRECEDES A READMIT BY 30 d, 90 d? For each case, is it followed by a readmission within 30 days or not
*/

drop table if exists NATHALIE.tmp2;

create table NATHALIE.tmp2 as
select * from NATHALIE.tmp_base;

insert into NATHALIE.tmp2 (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0);

--main computation
drop table if exists NATHALIE.PRJREA_STEP2_READMIT_LABELS
;

create table NATHALIE.PRJREA_STEP2_READMIT_LABELS as
SELECT 
    *
    , case
        when days_until_next_discharge <= 30 then 1 
        else 0 
      end as is_followed_by_a_30d_readmit
    , case
        when days_until_next_discharge <= 90 then 1 
        else 0 
      end as is_followed_by_a_90d_readmit
FROM
(-- A is earlier than B
	SELECT 
    	  A.*
    	, CASE
        		WHEN A.cin_no = B.cin_no AND  DATEDIFF(B.adm_dt, A.dis_dt) >= 0 THEN DATEDIFF(B.adm_dt, A.dis_dt)
        		ELSE NULL
    	    END AS days_until_next_discharge
    	, case
        	    when A.cin_no = B.cin_no then B.case_id
        	    else null
        	end as subsequent_stay_case_id
    FROM NATHALIE.tmp2 AS A LEFT JOIN NATHALIE.tmp_base AS B ON A.rownumber = B.rownumber - 1 
) AS S
;

/*
Clean up
*/

drop table if exists tmp;
drop table if exists tmp2;
drop table if exists tmp_base;