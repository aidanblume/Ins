/***
Title:              step11_analytic_set
Description:        Finalize readmission data set for predictive modeling and clean up workspace 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step10_ER 
Output:             NATHALIE.PRJREA_analytic_set
                    NATHALIE.PRJREA_analytic_set_LACE
***/

/*
FULL DATA SET  
*/

DROP TABLE IF EXISTS NATHALIE.PRJREA_ANALYTIC_SET;

CREATE TABLE NATHALIE.PRJREA_ANALYTIC_SET 
-- STORED AS PARQUET
AS
SELECT A.*, B3.SNFLTCSA_admits_this_period, C.ppg_members_this_period, "under revision" as lacare_members_this_period, to_date(now()) as dataset_dt
FROM
(
    select
        case_id
        , cin_no, adm_age, agegp_cty, agegp_hedis, agegp_lob_rollup, gender, language_written_code, ethnicity_code, zip_code, zip4, has_phone
        , product_name
        , segment
        , lob
        , ncqa_lob
        , ppg
        , ppg_name
        , dhs_site
        , dhs_service
        -- , pcp
        -- , site_no
        -- , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        -- , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20
        -- , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        -- , aprdrg, severity
        , primary_dx
        , isnull(PreviousMyocardialInfarction, 0) as PreviousMyocardialInfarction, isnull(CerebrovascularDisease, 0) as CerebrovascularDisease, isnull(PeripheralVascularDisease, 0) as PeripheralVascularDisease
        , isnull(DiabetesWithoutComplications, 0) as DiabetesWithoutComplications
        , isnull(CongestiveHeartFailure, 0) as CongestiveHeartFailure, isnull(DiabetesWithEndOrganDamage, 0) as DiabetesWithEndOrganDamage
        , isnull(ChronicPulmonaryDisease, 0) as ChronicPulmonaryDisease, isnull(MildLiverOrRenalDisease, 0) as MildLiverOrRenalDisease
        , isnull(AnyTumor, 0) as AnyTumor, isnull(Dementia, 0) as Dementia, isnull(ConnectiveTissueDisease, 0) as ConnectiveTissueDisease
        , isnull(AIDS, 0) as AIDS, isnull(ModerateOrSevereLiverOrRenalDisease, 0) as ModerateOrSevereLiverOrRenalDisease, isnull(MetastaticSolidTumor, 0) as MetastaticSolidTumor
        , isnull(surgery, 0) as surgery
        , prior_stay_case_id, prior_stay_los, days_since_prior_discharge, is_a_30d_readmit, is_a_90d_readmit
        , preadmitsnfltcsaname, type_preadmitsnfltcsa, days_since_preadmitsnfltcsa, snfltcsa_90dback, snfltcsa_14dback,  snfltcsa_7dback, snfltcsa_3dback, snfltcsa_1dback, snfltcsa_admitsthismonth 
        , postdischarge_snfltcsaname
        , case 
            when type_postdischargesnfltcsa is null then 'Home'
            else type_postdischargesnfltcsa
          end as general_location_of_post_discharge_care
        , days_until_snfltcsa, snfltcsa_90dfwd 
        , case 
                when days_until_next_admit <= 30 then 1  
                else 0   
            end as snfltcsa_30dfwd
        , case 
                when days_until_next_admit <= 21 then 1  
                else 0   
            end as snfltcsa_21dfwd
        , snf_14dfwd as snfltcsa_14dfwd, snf_7dfwd as snfltcsa_7dfwd, snf_3dfwd as snfltcsa_3dfwd, snf_1dfwd as snfltcsa_1dfwd
        , uniquemember_postdischargesnfltcsa_admitsthismonth, uniquemember_postdischargesnfltcsa_admitsthisperiod 
        , count_prior6m_er
        , from_er
        , adm_dt, dis_dt, los, hospname, dis_status
        , days_until_next_admit, subsequent_stay_case_id, subsequent_stay_los, is_a_30d_death
        , case 
                when days_until_next_admit <= 3 then 1  
                else 0   
            end as is_followed_by_a_3d_readmit
         , case 
                when days_until_next_admit <= 7 then 1  
                else 0   
            end as is_followed_by_a_7d_readmit
        , case 
                when days_until_next_admit <= 14 then 1  
                else 0   
            end as is_followed_by_a_14d_readmit
        , case 
                when days_until_next_admit <= 21 then 1  
                else 0   
            end as is_followed_by_a_21d_readmit
        , is_followed_by_a_30d_readmit, is_followed_by_a_90d_readmit
        , case 
                when days_until_next_admit <= 3 then subsequent_stay_los 
                else null
            end as 3dreadm_los
        , case 
                when days_until_next_admit <= 7 then subsequent_stay_los 
                else null
            end as 7dreadm_los
        , case 
                when days_until_next_admit <= 14 then subsequent_stay_los 
                else null
            end as 14dreadm_los
        , case 
                when days_until_next_admit <= 21 then subsequent_stay_los 
                else null
            end as 21dreadm_los
        , case 
                when is_followed_by_a_30d_readmit = 1 then subsequent_stay_los 
                else null
            end as 30dreadm_los
        , case 
                when is_followed_by_a_90d_readmit = 1 then subsequent_stay_los 
                else null
        end as 90dreadm_los
        , case 
          when is_followed_by_a_30d_readmit < is_a_30d_death then is_a_30d_death
          else is_followed_by_a_30d_readmit
        end as is_followed_by_death_or_readmit   --alt output label addition //TK is this correct???
    FROM NATHALIE.prjrea_step10_ER 
    -- Select 24 months of records. Discharge date must be specified and must be 6 months old, which allows for a 90 day post index discharge and for claims to come through. 
    where adm_dt >= add_months(now(), -30)
    -- and dis_dt <= add_months(now(), -6)
    and dies_before_discharge = 0 
    --and segment != 'CCI' // Also, ppg!=a DHS ppg; Note that this exclusion will be done in R while modeling, because scoring requires all cases, even those excluded from training. 
    --and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
) A
LEFT JOIN
( -- add general SNFLTCSA census info, preadmit
    select preadmitsnfltcsaname, sum(snfltcsa_admitsthismonth) as SNFLTCSA_admits_this_period
    from   
    (
        select distinct preadmitsnfltcsaname, yrmo, snfltcsa_admitsthismonth
        from
        (
            select preadmitsnfltcsaname, cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo, snfltcsa_admitsthismonth
            from NATHALIE.prjrea_step10_ER
            where adm_dt >= add_months(now(), -30)
            -- and adm_dt <= add_months(now(), -6) -- Count SNF admits till time window closes, regardless of discharge status
            -- and dis_dt <= add_months(now(), -6)
            and dies_before_discharge = 0 
            --and segment != 'CCI'
            --and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
            --and preadmitsnfltcsaname is not null
        ) B1
    ) B2
    group by preadmitsnfltcsaname
) B3
ON A.preadmitsnfltcsaname = B3.preadmitsnfltcsaname
-- LEFT JOIN
-- (
--     select postdischarge_snfname, sum(postdischarge_snf_admitsthismonth) as postdischarge_SNF_admits_this_period
--     from   
--     (
--         select distinct postdischarge_snfname, yrmo, postdischarge_snf_admitsthismonth
--         from
--         (
--             select postdischarge_snfname, cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo, postdischarge_snf_admitsthismonth
--             from NATHALIE.prjrea_step10_ER
--             where adm_dt >= add_months(now(), -30)
--             and adm_dt <= add_months(now(), -6) -- Count SNF admits till time window closes, regardless of discharge status
--             -- and dis_dt <= add_months(now(), -6)
--             and dies_before_discharge = 0 
--             --and segment != 'CCI'
--             --and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
--             --and postdischarge_snfname is not null
--         ) B4
--     ) B5
--     group by postdischarge_snfname
-- ) B6
-- ON A.postdischarge_snfname = B6.postdischarge_snfname
LEFT JOIN
( -- add general PPG census info
    select ppg
        , count(distinct cin_no) as ppg_members_this_period
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
    where term_dt >= add_months(now(), -30)
    -- and eff_dt <= add_months(now(), -6)
    group by ppg
) C
ON A.ppg = C.ppg
;

/*
L = LOS , length of stay
A = from_ER, acuity
C = several fields, comorbidities
E = ER_Visits, number of ER visits in prior 6 months.
*/

drop table if exists nathalie.PRJREA_ANALYTIC_SET_LACE
;

CREATE TABLE PRJREA_ANALYTIC_SET_LACE
-- STORED AS PARQUET
AS
SELECT 
    case_id
    --FILTERING: segment != 'CCI' when training but not when scoring
    , segment
    --FILTERING: exclude 'Y' when training but not when scoring
    , dhs_site 
    --LACE: LENGTH OF STAY
    , los     
    --LACE: ACUITY
    , from_er              
    -- LACE: COMORBIDITIES
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    --LACE: EMERGENCY
    , count_prior6m_er   
    --OUTPUT
    , is_followed_by_a_30d_readmit  
    -- --alt output label addition
    -- , is_followed_by_death_or_readmit   
    , dataset_dt
    -- , segment
FROM NATHALIE.PRJREA_ANALYTIC_SET
-- where segment <> 'CCI' or segment is null
;


/*
HEDIS --- to do
*/


/*
CLEAN UP
*/ 

-- drop table if exists nathalie.prjrea_step1_inpatient_cases; 
-- drop table if exists nathalie.prjrea_step2_readmit_labels;
-- drop table if exists nathalie.nathalie.prjrea_step3_diagnoses ;
-- drop table if exists nathalie.nathalie.prjrea_step4_procedures ;
-- drop table if exists nathalie.nathalie.prjrea_step5_ppg_lob_pcp ;
-- drop table if exists nathalie.nathalie.prjrea_step6_demog ; 
-- drop table if exists nathalie.nathalie.prjrea_step7_hospitals;
-- drop table if exists nathalie.prjrea_step8_snf;
-- drop table if exists nathalie.prjrea_step9_postdischargesnf;

