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
SELECT 
    *
    , case 
       when is_followed_by_a_30d_readmit < is_a_30d_death then is_a_30d_death
       else is_followed_by_a_30d_readmit
    end as is_followed_by_death_or_readmit   --alt output label addition
FROM NATHALIE.prjrea_step6_lace_comorbidities 
-- Select 24 months of records. Discharge date must be specified and must be 6 months old, which allows for a 90 day post index discharge and for claims to come through. 
where adm_dt >= add_months(now(), -30)
and dis_dt >= add_months(now(), -6)
and dies_before_discharge = 0 
and segment != 'CCI'
--and datediff(d.dis_dt,d.adm_dt)>0 -- single day admits?
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
    concat('cin', cin_no, '_adm', cast(adm_dt as string)) as rowid
    , 
    los                  --LACE: LENGTH OF STAY
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