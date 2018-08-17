/***
Title:              step3_LOB_PCP
Description:        Add LOB, PCP information to inpatient cases. Allows CCI exclusion.  
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.PRJREA_STEP2_READMIT_LABELS 
                    hoap.MEMMO
                    nathalie.ref_lob
Output:             NATHALIE.prjrea_step3_lob_pcp
***/


-- ATTRIBUTES AT TIME OF ADMISSION: LOB (with name added), segment, pcp, site_no

drop table if exists NATHALIE.prjrea_step3_lob_pcp
;

set max_row_size = 7mb
;

create table NATHALIE.prjrea_step3_lob_pcp
as
select *
from 
(
    select --distinct
        UNIQUE_CASES.*
        , MEMMO.pcp
        , MEMMO.site_no 
        , MEMMO.product_code
        , LOB.output as product_name
        , MEMMO.segment
        , row_number() over(partition by unique_cases.cin_no, UNIQUE_CASES.adm_dt, UNIQUE_CASES.dis_dt order by process_date desc) as rownumner3297
    from
    (
        select *, concat(cast(date_part('year', adm_dt) as varchar(4)), lpad(cast(date_part('month', adm_dt) as varchar(4)), 2, '0')) as adm_yearmth
        from
        nathalie.PRJREA_STEP2_READMIT_LABELS 
        where case_id is not null --drops rows of padding from previous step
    ) UNIQUE_CASES 
    left join
    (
        select *
        from HOAP.memmo
    ) as MEMMO
    on UNIQUE_CASES.cin_no = MEMMO.cin_no
    and UNIQUE_CASES.adm_yearmth = MEMMO.yearmth
    left join 
    nathalie.ref_lob as LOB
    on MEMMO.product_code = LOB.input
) S
where rownumner3297 = 1
;

/*
CLEAN UP
*/

set max_row_size = 1mb
;
