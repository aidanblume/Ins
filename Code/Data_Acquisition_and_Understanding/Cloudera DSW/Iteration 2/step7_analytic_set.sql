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
-- STORED AS PARQUET
AS
SELECT A.*, B3.SNFLTCSA_admits_this_period, C.ppg_members_this_period, D.lacare_members_this_period
FROM
(
    select
        concat(cin_no, cast(adm_dt as string)) as case_id
        , cin_no, adm_age, agegp_cty, agegp_hedis, agegp_lob_rollup, gender, language_written_code, ethnicity_code, zip_code, zip4, has_phone
        -- , product_name
        , segment
        , lob
        , ppg
        , ppg_name
        -- , pcp
        -- , site_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , aprdrg, severity
        , previousmyocardialinfarction, cerebrovasculardisease, peripheralvasculardisease, diabeteswithoutcomplications, congestiveheartfailure
        , diabeteswithendorgandamage, chronicpulmonarydisease, mildliverorrenaldisease, anytumor, dementia, connectivetissuedisease, aids
        , moderateorsevereliverorrenaldisease, metastaticsolidtumor
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
        , count_prior6m_er, from_er
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
    FROM NATHALIE.prjrea_step6_lace_comorbidities 
    -- Select 24 months of records. Discharge date must be specified and must be 6 months old, which allows for a 90 day post index discharge and for claims to come through. 
    where adm_dt >= add_months(now(), -30)
    -- and dis_dt <= add_months(now(), -6)
    and dies_before_discharge = 0 
    --and segment != 'CCI'
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
            from NATHALIE.prjrea_step6_lace_comorbidities
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
--             from NATHALIE.prjrea_step6_lace_comorbidities
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
CROSS JOIN -- add general membership count
(
    select count(distinct P.cin_no) as lacare_members_this_period
    from 
    NATHALIE.prjrea_step6_lace_comorbidities Q
    left join
    (
        select Q2.carriermemid as cin_no, Q1.eff_dt, Q1.term_dt
        from edwp.mem_prov_asgnmt_hist as Q1
        left join 
        plandata.enrollkeys as Q2
        on Q1.MEM_BUS_KEY_NUM = Q2.memid
        where substr(Q1.MEM_BUS_KEY_NUM, 1, 3) = 'MEM'
        union 
        select MEM_BUS_KEY_NUM, eff_dt, term_dt
        from edwp.mem_prov_asgnmt_hist
        where substr(MEM_BUS_KEY_NUM, 1, 3) != 'MEM'
    ) P
    on Q.cin_no=P.cin_no
    where P.term_dt >= add_months(now(), -30)
    -- and P.eff_dt <= add_months(now(), -6)
) D
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
-- STORED AS PARQUET
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