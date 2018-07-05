/***
Title:              step4c_PPG
Description:        Adds member's PPG assignments at readmit date to a data set of acute inpatient cases (=stays). 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4b_hospitals 
                    edwp.mem_prov_asgnmt_hist
Output:             nathalie.prjrea_step4c_PPG to non-aggregated data set
                    -- nathalie.prjrea_tblo_readmit_PPG for readmission rates by PPG
***/

/*
ATTACH CONCURRENT PPG TO EACH ADMIT CASE 
*/

drop table if exists nathalie.prjrea_step4c_PPG
;

create table nathalie.prjrea_step4c_PPG
as
select 
    A.*
    , B.PPG, B.PPG_EFF_DT, B.PPG_TERM_DT
from NATHALIE.prjrea_step4b_hospitals as A
left join 
(
    select *
    from 
    (
        select IP.*
            , PPG.EFF_DT as PPG_EFF_DT
            , PPG.TERM_DT as PPG_TERM_DT
            , PPG.PPG as PPG
            , row_number() over(partition by case_id order by PPG.EFF_DT desc) as rownumber2
        from NATHALIE.prjrea_step4b_hospitals as IP
        left join 
        ( --Bring in PPG assignments
            select  distinct
                MEM_BUS_KEY_NUM
                , PPG
                , EFF_DT
                , TERM_DT 
            from edwp.mem_prov_asgnmt_hist
        ) as PPG
        on IP.cin_no = PPG.MEM_BUS_KEY_NUM
        where IP.adm_dt >= PPG.EFF_DT 
        and (IP.adm_dt < PPG.TERM_DT or PPG.TERM_DT is null)
    ) S
    where rownumber2 = 1
) as B
on A.case_id = B.case_id
;

-- /* 
-- EXCLUDE CASES
-- --Exclude CCI --> for more accurate report of readmission rates. Counts need to come from unfiltered data set. 
-- */

-- drop table if exists nathalie.tmp_no_cci
-- ;

-- create table nathalie.tmp_no_cci
-- as
-- select *
-- from nathalie.prjrea_step4c_PPG 
-- where segment != 'CCI'
-- ;


-- /*
-- SUMMARIZE: PPG
-- */

-- drop table if exists nathalie.prjrea_tblo_readmit_PPG
-- ;

-- create table nathalie.prjrea_tblo_readmit_PPG
-- as
-- select A.ppg as ppg, admit_count, readmission_rate as no_cci_readmission_rate, admit_count * readmission_rate as calculated_readmit_count
-- from 
-- (   
--     select 
--         ppg
--         , count(*) as admit_count
--     from prjrea_step4c_PPG
--     group by ppg
-- ) as A
-- left join
-- ( -- rates are derived without cci for accuracy. 
--     select 
--         ppg
--         , sum(is_a_30d_readmit) as number_readmits
--         , count(*) as number_all_admits
--         , round(sum(is_a_30d_readmit) / count(*), 2) as readmission_rate
--     from nathalie.tmp_no_cci
--     group by ppg
-- ) as B
-- on A.ppg = B.ppg
-- ;
