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
select 
    UNIQUE_CASES.*
    , MEMMO.pcp
    , MEMMO.site_no 
    , MEMMO.product_code
    , LOB.output as product_name
    , MEMMO.segment
from
(
    select *, concat(cast(date_part('year', adm_dt) as varchar(4)), lpad(cast(date_part('month', adm_dt) as varchar(4)), 2, '0')) as adm_yearmth
    from
    nathalie.PRJREA_STEP2_READMIT_LABELS 
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
;

/*
CLEAN UP
*/

set max_row_size = 1mb
;