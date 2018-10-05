/***
Title:              step3_PPG_LOB_PCP
Description:        Add PPG, Product, Segment, PCP assignment at time of admit 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step2_readmit_labels
                    plandata.enrollkeys
                    edwp.vw_grp_cd_segmtn
                    plandata.eligibilityorg
                    plandata.affiliation
                    plandata.provider
Output:             nathalie.prjrea_step3_PPG_LOB_PCP
Notes:              Can be improved by bringing in pcp info. Search below for /*TK HERE YOU HAVE OPPORTUNITY TO BRING IN PCP
***/


-- get enrollkey line with enrollid, ratecode, eff & term dates

drop table if exists nathalie.lob_step1;

create table nathalie.lob_step1 as
select 
    ra.case_id
    , ra.source_table --added only for debugging
    , ra.cin_no
    , ra.adm_dt
    , ra.dis_dt
    , ek.effdate
    , ek.termdate
    , ek.ratecode
    , ek.createdate
    , ek.lastupdate
    , ek.planid
    , ek.eligibleorgid
    , ek.enrollid
from nathalie.prjrea_step2_readmit_labels as ra
left join
-- inner join
(
    select carriermemid, effdate, termdate, ratecode, createdate, lastupdate, planid, eligibleorgid, enrollid
    from plandata.enrollkeys 
    where segtype = 'INT' and ratecode <> 'CMCWELL'
) ek
on ra.cin_no = ek.carriermemid
and ra.adm_dt >= ek.effdate
and ra.adm_dt <= ek.termdate
;




-- for instances with multiple matching enrollkey rows, keep the enrollkeys row with the latest `lastupdate` timestamp
-- And where there are still matching keys, order by segment (only 1 such case, nd ordering removes null & preserves spd)
drop table if exists nathalie.lob_step2;

create table nathalie.lob_step2 as
select case_id, max(lastupdate) as max_lastupdate from nathalie.lob_step1 group by case_id
;





drop table if exists nathalie.prjrea_step3_PPG_LOB_PCP;

create table nathalie.prjrea_step3_PPG_LOB_PCP 
as
select 
    A.*
    , B.segment as tmp_segment
    , B.lob as tmp_lob
    , B.ppg as tmp_ppg
    , B.ppg_name as tmp_ppg_name
from nathalie.prjrea_step2_readmit_labels as A
left join
(
    select 
        has_planid_or_null.case_id
        , has_planid_or_null.cin_no
        , has_planid_or_null.adm_dt
        , has_planid_or_null.dis_dt
        , seg.segmtn as segment
        , eo.lob
        , ipa.ppg
        , ipa.enty_prov_nm as ppg_name
        , row_number() over (partition by has_planid_or_null.case_id order by seg.segmtn) as rn
    from 
    (
        select *
        from 
        (
            select *, row_number() over (partition by case_id order by planid) as rn0
            from 
            (
                select nat.* 
                from nathalie.lob_step1 nat
                inner join
                nathalie.lob_step2 s2
                on nat.case_id = s2.case_id and nat.lastupdate = s2.max_lastupdate
                union
                select nat2.*
                from nathalie.lob_step1 nat2
                where planid is null
            ) max_or_null_all
        ) max_or_null_dedup
        where rn0=1
    ) has_planid_or_null
    --/***get MCLA segment, NULL for CMC and LACC***/
    left join
    (
        select * 
        from edwp.vw_grp_cd_segmtn /*where lob='MCLA' and segment in ('CCI','MCE','TANF','SPD')*/
    ) seg
    on has_planid_or_null.ratecode = seg.grp_cd
    
   --/*** get LOB ***/
    left join
    (
        select distinct
            eligibleorgid,
            case
                when trim(fullname)='COVERED CALIFORNIA' then 'LACC'
                when trim(fullname)='LA CARE COVERED DIRECT' then 'LACC'
                when trim(fullname)='CMC SPONSOR' then 'CMC'
                when trim(fullname)='KAISER PERMANENTE' then 'KAISER'
                when trim(fullname)='ANTHEM BLUE CROSS OF CA MEDI-CAL' then 'ANTHEM'
                when trim(fullname)='CARE 1ST HEALTH PLAN MEDI-CAL' then 'CARE 1ST'
                else trim(fullname)
              end as lob
        from plandata.eligibilityorg
    ) eo
    on has_planid_or_null.eligibleorgid = trim(eo.eligibleorgid)
    
    /***PPG data***/
    left join
    (
        select
            ppg1.enrollid
            , ppg1.ppg
            , v.enty_prov_nm
        from
        (
            select distinct
                mp.enrollid
                , regexp_replace(ap.ppg_fullname, '.*\\([A-z]+-|-.*\\)', '') as ppg
            from plandata.memberpcp mp 
            inner join
            plandata.enrollkeys ek3
            on mp.enrollid = ek3.enrollid and mp.termdate = ek3.termdate
            inner join
            (  /*TK HERE YOU HAVE OPPORTUNITY TO BRING IN PCP*/
                select 
                    aff.affiliationid as pcpaffiliationdid
                    , aff.affiliateid
                    , p1.provid as ppg_provid
                    , p1.fullname as ppg_fullname
                from plandata.affiliation aff
                inner join
                plandata.provider p1
                on aff.affiliateid = p1.provid
            ) ap
            on mp.affiliationid = ap.pcpaffiliationdid
            where mp.pcptype = 'PCP'
        ) ppg1
        inner join
        edwp.vw_ppg v 
        on ppg1.ppg = v.prov_bus_key_num
    ) ipa
    on has_planid_or_null.enrollid = ipa.enrollid
) as B
on A.case_id=B.case_id
where B.rn=1
;

--clean up

drop table if exists nathalie.lob_step1;
drop table if exists nathalie.lob_step2;

