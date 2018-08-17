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
            select distinct ppg, cin_no, eff_dt, term_dt
            from 
            (
            	select A.ppg, B.carriermemid as cin_no, A.eff_dt, A.term_dt
            -- 	select regexp_replace(A.ppg,'.*\\([A-z]+-|-.*\\)', '') as ppg, B.carriermemid as cin_no, A.eff_dt, A.term_dt --makes no dif
            	from edwp.mem_prov_asgnmt_hist as A
            	left join 
            	plandata.enrollkeys as B
            	on A.MEM_BUS_KEY_NUM = B.memid
            	where substr(A.MEM_BUS_KEY_NUM, 1, 3) = 'MEM'
            	union 
            	select ppg, MEM_BUS_KEY_NUM, eff_dt, term_dt
            	from edwp.mem_prov_asgnmt_hist
            	where substr(MEM_BUS_KEY_NUM, 1, 3) != 'MEM'
            ) as PPG_S
	    ) as PPG
        on IP.cin_no = PPG.cin_no
        where IP.adm_dt >= PPG.EFF_DT 
        and (IP.adm_dt < PPG.TERM_DT or PPG.TERM_DT is null)
    ) S
    where rownumber2 = 1
) as B
on A.case_id = B.case_id
;