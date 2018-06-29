/***
Title:              step4d_SNF
Description:        Adds member's SNF if the member was housed at a SNF at any time during the 90 d that precede current hospital admit date *OR* after the last inpatient discharge, whichever was more recent. 
                    If a member is admitted to several SNFs during this period, then each SNF is attached to the case that is sent to TABLEAU so that each may share in the responsibility for the readmission.
                    Another file is created that saves the most recent SNF as well as aggregate values to reflect multiple SNF assignments for the same case. This file has 1 row per inpatient case. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4c_PPG 
Output:             nathalie.prjrea_step4d_SNF has most recent SNF. Contains 1 row per inpatient case. This is the file that is being built for modeling purposes. 
                    nathalie.prjrea_tblo_readmit_SNF has several rows per inpatient case when the inpatient case was preceded by several valid SNF admits. 
                    [Note that unlike other tables meant for Tableau, here: Aggregate tables showing rate by SNF for period of N days preceding readmission are computed on the fly in Tableau rather than here]
Issues:             Alternative coding. An alternative method may be: VIA REFERRALID FIELD IN QNXT. Elected not to do because the method used seemed more direct.
                    (Steps in that alt meth: CASE to CLAIM CROSSWALK TABLE; ATTACH REFERRALID and ADMITSOURCE to DATA;
                    select claimid, referralid, admitsource from plandata.claim where [claimid matches one in the case crosswalk table]; ATTACH SNF NAME TO DATA)
***/

/*
ATTACH RECENT SNFs TO EACH ADMIT CASE 
*/

create table nathalie.tmp
as
--For each inpatient case, attach to it (if any) the most recent SNF case that happened within the 90 day window preceding admission.
select All_inp.*
    , S2.snf_90dback
    , S2.SNF
    , S2.days_since_SNF
    , S2.adm_dt_SNF
    , S2.dis_dt_SNF
from prjrea_step4c_PPG as All_inp
left join
( -- select only 1 episode per SNF per case (avoid representing the same SNF multiple times per case)
    select *
    from 
    ( -- Select SNF Cases that were active during the 90 day pre-inpatient admission window
        select IP.cin_no
            , IP.adm_dt
            , IP.dis_dt
            , SNF.adm_dt as adm_dt_SNF
            , SNF.dis_dt as dis_dt_SNF
            , SNF.provider as SNF
            , datediff(IP.adm_dt, SNF.dis_dt) as days_since_SNF
            , 1 as snf_90dback
            -- , row_number() over(partition by IP.case_id order by SNF.dis_dt desc) as rownumber --remove this line because it makes more sense to keep all SNFs in the inter-hospitalization period rather than just the most recent one.
            , row_number() over(partition by IP.cin_no, IP.adm_dt, IP.dis_dt, SNF.provider order by SNF.dis_dt desc) as rownumber -- keep the most recent valid stay for each SNF so you avoid having several rows for the same SNF when a member went in and out of it repeatedly.
        from prjrea_step4c_PPG as IP
        left join 
        (-- Select unique SNF cases (did not look for contiguous ones)
            select distinct case_id, cin_no, adm_dt, dis_dt, provider
            from
            ( --add number rows inside partitions where each partition is a unique (cin_no, admi_dt, dis_dt) tupple
                select case_id, cin_no, adm_dt, dis_dt, provider, source_table
                , row_number() over(partition by cin_no, adm_dt, dis_dt order by source_table asc, case_id desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select case_id, cin_no, adm_dt, dis_dt, provider
                    , 1 as source_table
                    from `hoap`.`QNXT_CASE_INPSNF`
                    where srv_cat = '04snf'
                    union
                    select case_id, cin_no, adm_dt, dis_dt, provider
                    , 2 as source_table
                    from `hoap`.`clm_case_inpsnf`
                    where srv_cat = '04snf'
                    union
                    select case_id, cin_no, adm_dt, dis_dt, provider
                    , 3 as source_table
                    from `hoap`.`ENC_CASE_INPSNF`
                    where srv_cat = '04snf'
               ) AS ALL_CASES
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) as SNF
        on IP.cin_no = SNF.cin_no
        where IP.adm_dt >= SNF.adm_dt --keep SNF that started before the IP admit (eliminate SNF that began after IP)
        and IP.adm_dt < adddate(SNF.dis_dt, 90) --keep SNF that had yet not ended 90 day before IP admit (eliminate SNF stays that ended long before the IP began)
        and datediff(IP.adm_dt, SNF.dis_dt) < IP.days_since_prior_discharge --keep SNF that were active after the last hospitalization (essentially, every hospitalization resets the clock/list of SNFs responsible for preventing the next hospitalization)
    ) S1
    where S1.rownumber = 1
) S2
on All_inp.cin_no = S2.cin_no and All_inp.adm_dt = S2.adm_dt and All_inp.dis_dt = S2.dis_dt
;

/*
REDUCE TO 1 ROW PER HOSPITAL ADMIT
nathalie.prjrea_step4d_SNF has most recent SNF. Contains 1 row per inpatient case. This is the file that is being built for modeling purposes. 
*/

set max_row_size=7mb;

drop table if exists prjrea_step4d_SNF
;

create table prjrea_step4d_SNF
as
select All_inp.*
        , S2.snf_90dback
        , S2.SNF
        , S2.days_since_SNF
        , S2.adm_dt_SNF
        , S2.dis_dt_SNF
from prjrea_step4c_PPG as All_inp
left join
( --select the most recent SNF when there is any SNF
    select *
    from
    (
        select *
            , row_number() over(partition by case_id, cin_no, adm_dt, dis_dt order by dis_dt_snf desc) as rownumber2 
        from nathalie.tmp
        where snf_90dback = 1 -- not necessary but emphasizes that only admits preceded by a SNF since last discharge or within 90 days of admit are included
    ) S1
    where rownumber2 = 1
)S2
on All_inp.cin_no = S2.cin_no and All_inp.adm_dt = S2.adm_dt and All_inp.dis_dt = S2.dis_dt
;

set max_row_size=1mb; 

/*
GENERATE FILE FOR TABLEAU
nathalie.prjrea_tblo_readmit_SNF has several rows per inpatient case when the inpatient case was preceded by several valid SNF admits. 
Not all fields are required
[Note that unlike other tables meant for Tableau, here: Aggregate tables showing rate by SNF for period of N days preceding readmission are computed on the fly in Tableau rather than here]
*/

drop table if exists nathalie.prjrea_tblo_readmit_SNF
;

create table nathalie.prjrea_tblo_readmit_SNF
as
select case_id, cin_no, adm_dt, dis_dt, SNF, days_since_SNF, adm_dt_SNF, dis_dt_SNF, dies_before_discharge, is_a_30d_death, is_a_30d_readmit, is_a_90d_readmit, product_code, product_name, segment
from nathalie.tmp
where snf_90dback = 1 -- not necessary but emphasizes that only admits preceded by a SNF since last discharge or within 90 days of admit are included
;

/*
CLEAN UP
*/

drop table if exists nathalie.tmp
;