/***
Title:              step8_tableau_sets
Description:        Create sets for tableau dashboards 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.PRJREA_analytic_set 
Output:             NATHALIE.PRJREA_...
***/

-- /*
-- DASHBOARD1A
-- */

-- drop table if exists NATHALIE.PRJREA_DASHBOARD1_SOURCE
-- ;

-- CREATE TABLE NATHALIE.PRJREA_DASHBOARD1_SOURCE
-- STORED AS PARQUET
-- AS
-- SELECT 
--     case_id
--     , adm_dt
--     , los                  --LACE: LENGTH OF STAY
--     , aprdrg
--     , product_name
--     , segment
--     , hospname
--     , ppg
--     , ppg_members_this_period
--     , snfname
--     , days_since_snf
--     -- , snf_90dback
--     , snf_1dback
--     -- , snf_3dback
--     -- , snf_7dback
--     -- , snf_14dback
--     , snf_admitsthismonth
--     , SNF_admits_this_period
--     , lacare_members_this_period
--     , is_a_30d_readmit
--     , is_a_90d_readmit
--     , is_followed_by_a_30d_readmit
--     , is_followed_by_a_90d_readmit    
--     , case 
--             when is_followed_by_a_30d_readmit = 1 then subsequent_stay_los 
--             else null
--         end as 30dreadm_los
--     , case 
--             when is_followed_by_a_90d_readmit = 1 then subsequent_stay_los 
--             else null
--         end as 90dreadm_los
-- FROM NATHALIE.PRJREA_ANALYTIC_SET
-- ;





/*
DASHBOARD2Aii
*/

drop table if exists NATHALIE.PRJREA_DASHBOARD2Aii_SOURCE
;

CREATE TABLE NATHALIE.PRJREA_DASHBOARD2Aii_SOURCE
STORED AS PARQUET
AS
SELECT 
    case_id
    , cin_no
    , adm_dt
    , los as LOS               
    , aprdrg
    , product_name
    , segment
    , hospname
    -- , ppg
    -- , ppg_members_this_period
    -- , snfname as snf_name
    -- , days_since_snf
    -- , snf_90dback
    -- , snf_1dback
    -- , snf_3dback
    -- , snf_7dback
    -- , snf_14dback
    -- , snf_admitsthismonth as snf_admits_this_month
    -- , SNF_admits_this_period
    -- , lacare_members_this_period
    , postdischarge_snfname
    , days_until_snf
    , snf_90dfwd
    , snf_1dfwd 
    , snf_3dfwd 
    , snf_7dfwd 
    , snf_14dfwd 
    -- , postdischarge_snf_admitsthismonth
    , postdischarge_SNF_admits_this_period
    , uniquemember_SNF_admits_thisPeriod
    -- , is_a_30d_readmit
    -- , is_a_90d_readmit
    , is_followed_by_a_30d_readmit
    , is_followed_by_a_90d_readmit    
    , case 
            when is_followed_by_a_30d_readmit = 1 then subsequent_stay_los 
            else null
        end as 30d_readmit_LOS
    , case 
            when is_followed_by_a_90d_readmit = 1 then subsequent_stay_los 
            else null
        end as 90d_readmit_LOS
FROM NATHALIE.PRJREA_ANALYTIC_SET
;

