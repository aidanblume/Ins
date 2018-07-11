/***
Title:              step7_analytic_set
Description:        Finalize readmission data set for predictive modeling and clean up workspace 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step6_lace_comorbidities 
Output:             NATHALIE.PRJREA_analytic_set
                    NATHALIE.PRJREA_analytic_set_LACE
***/

/*
FULL DATA SET
*/

DROP TABLE IF EXISTS NATHALIE.PRJREA_ANALYTIC_SET;

CREATE TABLE NATHALIE.PRJREA_ANALYTIC_SET 
STORED AS PARQUET
AS
SELECT A.*, B3.SNF_admits_this_period, C.ppg_members_this_period
FROM
(
    select
        concat(cin_no, cast(adm_dt as string)) as case_id
        , cin_no, adm_age, agegp_cty, agegp_hedis, agegp_lob_rollup, gender, language_written_code, ethnicity_code, zip_code, zip4, has_phone
        , product_name, segment, ppg, pcp, site_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , aprdrg, severity
        , previousmyocardialinfarction, cerebrovasculardisease, peripheralvasculardisease, diabeteswithoutcomplications, congestiveheartfailure
        , diabeteswithendorgandamage, chronicpulmonarydisease, mildliverorrenaldisease, anytumor, dementia, connectivetissuedisease, aids
        , moderateorsevereliverorrenaldisease, metastaticsolidtumor
        , prior_stay_case_id, prior_stay_los, days_since_prior_discharge, is_a_30d_readmit, is_a_90d_readmit
        , snfname, days_since_snf, snf_90dback, snf_14dback, snf_7dback, snf_3dback, snf_1dback, snf_admitsthismonth
        , count_prior6m_er, from_er
        , adm_dt, dis_dt, los, hospname, dis_status
        , days_until_next_discharge, subsequent_stay_case_id, subsequent_stay_los, is_a_30d_death, is_followed_by_a_30d_readmit, is_followed_by_a_90d_readmit
        , case 
          when is_followed_by_a_30d_readmit < is_a_30d_death then is_a_30d_death
          else is_followed_by_a_30d_readmit
        end as is_followed_by_death_or_readmit   --alt output label addition //TK is this correct???
    FROM NATHALIE.prjrea_step6_lace_comorbidities 
    -- Select 24 months of records. Discharge date must be specified and must be 6 months old, which allows for a 90 day post index discharge and for claims to come through. 
    where adm_dt >= add_months(now(), -30)
    and dis_dt <= add_months(now(), -6)
    and dies_before_discharge = 0 
    --and segment != 'CCI'
    --and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
) A
LEFT JOIN
(
    select snfname, sum(snf_admitsthismonth) as SNF_admits_this_period
    from   
    (
        select distinct snfname, yrmo, snf_admitsthismonth
        from
        (
            select snfname, cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo, snf_admitsthismonth
            from NATHALIE.prjrea_step6_lace_comorbidities
            where adm_dt >= add_months(now(), -30)
            and adm_dt <= add_months(now(), -6) -- Count SNF admits till time window closes, regardless of discharge status
            -- and dis_dt <= add_months(now(), -6)
            and dies_before_discharge = 0 
            --and segment != 'CCI'
            --and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
            --and snfname is not null
        ) B1
    ) B2
    group by snfname
) B3
ON A.snfname = B3.snfname
LEFT JOIN
(
    select ppg, count(distinct ppg, cin_no) as ppg_members_this_period
    from 
    (
        select A.ppg, B.carriermemid as cin_no, A.eff_dt, A.term_dt
        from edwp.mem_prov_asgnmt_hist as A
        left join 
        plandata.enrollkeys as B
        on A.MEM_BUS_KEY_NUM = B.memid
        where substr(A.MEM_BUS_KEY_NUM, 1, 3) = 'MEM'
        union 
        select ppg, MEM_BUS_KEY_NUM, eff_dt, term_dt
        from edwp.mem_prov_asgnmt_hist
        where substr(MEM_BUS_KEY_NUM, 1, 3) != 'MEM'
    ) S1
    -- where (term_dt is null OR term_dt >= add_months(now(), -30))
    -- and eff_dt <= add_months(now(), -6)
    where term_dt >= add_months(now(), -30)
    and eff_dt <= add_months(now(), -6)
    group by ppg
    order by ppg_members_this_period desc
) C
ON A.ppg = C.ppg
;

/*
L = LOS , length of stay
A = from_ER, acuity
C = several fields, comorbidities
E = ER_Visits, number of ER visits in prior 6 months.
*/

drop table if exists NATHALIE.PRJREA_ANALYTIC_SET_LACE
;

CREATE TABLE PRJREA_ANALYTIC_SET_LACE
STORED AS PARQUET
AS
SELECT 
    case_id
    , los                  --LACE: LENGTH OF STAY
    , case 
            when from_er = 'Y' then 1 
            else 0
        end as acuity                  --LACE: ACUITY
    --LACE: COMORBIDITIES
    , 
    PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , count_prior6m_er        --LACE: EMERGENCY
    , is_followed_by_a_30d_readmit                  --OUTPUT
    , is_followed_by_death_or_readmit   --alt output label addition
FROM NATHALIE.PRJREA_ANALYTIC_SET
;

/*
TABLEAU
*/

drop table if exists NATHALIE.PRJREA_TABLEAU_EXTRACT
;

CREATE TABLE NATHALIE.PRJREA_TABLEAU_EXTRACT
STORED AS PARQUET
AS
SELECT 
    case_id
    , los                  --LACE: LENGTH OF STAY
    , aprdrg
    , product_name
    , segment
    , hospname
    , ppg
    , 9 as ppg_membersthismonth --tk improvement would be to get census on a monthly basis
    , ppg_members_this_period
    , snfname
    , days_since_snf
    , snf_90dback
    , snf_1dback
    , snf_3dback
    , snf_7dback
    , snf_14dback
    , snf_admitsthismonth
    , SNF_admits_this_period
    , is_a_30d_readmit
    , is_a_90d_readmit
    , is_followed_by_a_30d_readmit
    , is_followed_by_a_90d_readmit    
    , case 
            when is_followed_by_a_30d_readmit = 1 then subsequent_stay_los 
            else null
        end as 30dreadm_los
    , case 
            when is_followed_by_a_90d_readmit = 1 then subsequent_stay_los 
            else null
        end as 90dreadm_los
FROM NATHALIE.PRJREA_ANALYTIC_SET
;

/*
CLEAN UP
*/

drop table if exists nathalie.prjrea_step1_inpatient_cases;
drop table if exists nathalie.prjrea_step2_readmit_labels;
drop table if exists nathalie.prjrea_step3_lob_pcp;
drop table if exists nathalie.prjrea_step4a_demog;
drop table if exists nathalie.prjrea_step4b_hospitals;
drop table if exists nathalie.prjrea_step4c_ppg;
drop table if exists nathalie.prjrea_step4d_snf;
drop table if exists nathalie.prjrea_step5_er;
drop table if exists nathalie.prjrea_step6_lace_comorbidities;
