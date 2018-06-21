/***
Title:              step1_dedup_inpatient_cases_from_HOAP
Description:        Generates a data set of acute inpatient cases (=stays) from the HOAP.HOA "case" tables. 
                    Eliminates duplicates and merges cases that are contiguous by 1 day. 
                    Is 1st step in generating analytic data sets for readmission rate computation.
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        HOAP.HOA QNXT, CLM and ENC case tables. 
Output:             NATHALIE.prjrea_step1_inpatient_cases
Notes:      		1. Inpatient cases are identified with 'where srv_cat = '01ip_a' on the sace files. 
					This excludes more SNF inpatient stays than using substr(type_bill,1,2) in ('11','12') on the hdr files.
					2. Unique tupple (cin_no, admi_dt) are selected with priority (1) later disc_dt, and (2) QNXT>CLM>ENC ** Note that this departs from the SAS script received in 2017
					3. There is a potential loss of Dx, Pr and provider information when cases are deduped over cin_no and admit_dt alone. Pr and Dx info will be retrieved again later. 
					4. Some overlapping stays remain by Iteration 16. See, e.g. cin_no '93066483A' for whom stays are complex possibly because some SNFs have not been eliminated from capture. 
					5. For improvements: see email from Chee <Thu 6/21/2018 9:45 AM> advocating for non-HOAP use. 
***/

/*
UNIQUE_CASES
Purpose:    To merge data from HOA's 3 case tables with priority QNXT>CLM>ENC
*/

create table NATHALIE.TMP_UNIQUE_CASES 
as
-- select only 1 with same (cin_no, admi_dt, dis_dt) tupple
-- add row number by cin_no partition. Will be used at next setp. 
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2 
from
(
    select *
    from
    ( --add number rows inside partitions where each partition is a unique (cin_no, admi_dt, dis_dt) tupple
        select *
        -- Departure from the original SAS script used for the ELM. Instead of:
        -- , row_number() over(partition by cin_no, adm_dt, dis_dt order by source_table asc, case_id desc) as rownumber
        --the following line is used:
        , row_number() over(partition by cin_no, adm_dt order by dis_dt desc, source_table asc, case_id desc) as rownumber
        --which uniques by cin_no and adm_dt, not dis_dt as well, and results in sources tables being prefered against the stated hierarchy
        from
        ( -- union of cases across 3 data tables: qnxt, clm, enc
            select case_id, adm_dt, dis_dt, cin_no
            , case_dx1
            , case_pr1
            , severity, aprdrg, dis_status, provider, paid_amt_case, from_er
            , 1 as source_table
            from hoap.QNXT_CASE_INPSNF as Q
            where srv_cat = '01ip_a'
            union
            select case_id, adm_dt, dis_dt, cin_no
            , case_dx1
            , case_pr1
            , severity, aprdrg, dis_status, provider, paid_amt_case, from_er
            , 2 as source_table
            from hoap.clm_case_inpsnf as C
            where srv_cat = '01ip_a'
            union
            select case_id, adm_dt, dis_dt, cin_no
            , case_dx1 
            , case_pr1
            , severity, aprdrg, dis_status, provider, null as paid_amt_case, from_er
            , 3 as source_table
            from hoap.ENC_CASE_INPSNF as E
            where srv_cat = '01ip_a'
       ) AS ALL_CASES
        order by cin_no, adm_dt, dis_dt
    ) ALL_CASES_PARTITIONED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, 4 as source_table
                , 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber =  1
;

/*
FUSE INITIAL STAY WITH TRANSFERS or 1d READMITS 

Purpose:    To merge rows that concern contiguous stays (also to capture cases that overlap in time). Contiguiity = discharge and admit are at most 1 day apart. 
Notes:      1. Awkward implementation because looping is not permitted in Impala environment (see https://stackoverflow.com/questions/49523380/write-a-while-loop-in-impala-sql)
            Therefore as long as you need to fuse admits, you need to hard-code the repetition of the search-and-fuse script below. 
            2. FS = "first stay" and SS = "second stay"
            3. What is kept:
                From FS in fusing contiguous stays: fromER, Dx1, Pr1, severity and aprdrg 
                From SS in fusing contiguous stays: discharge date from 2nd stay, and max(dis_dt). 
                What is concatenated (both FS and SS values are kept): case_id, source tables, provider. 
*/

-- Iteration 1

create table NATHALIE.TMP_FUSED_CASES_1
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_UNIQUE_CASES as FS
                inner join
                NATHALIE.TMP_UNIQUE_CASES as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

/*
The next set of 'Iteration x' are identical to Iteration 1 excpet for the table names. If the logic of Iteration 1 is valid so is the logic of Iteration x. 
*/

-- Iteration 2

create table NATHALIE.TMP_FUSED_CASES_2
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_1 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_1 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;
    
-- Iteration 3

create table NATHALIE.TMP_FUSED_CASES_3
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_2 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_2 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;
   
-- Iteration 4

create table NATHALIE.TMP_FUSED_CASES_4
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_3 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_3 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 5

create table NATHALIE.TMP_FUSED_CASES_5
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_4 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_4 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 6

create table NATHALIE.TMP_FUSED_CASES_6
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_5 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_5 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

-- Iteration 7 

create table NATHALIE.TMP_FUSED_CASES_7
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_6 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_6 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;


--Iteration 8

create table NATHALIE.TMP_FUSED_CASES_8
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_7 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_7 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;


--Iteration 9

create table NATHALIE.TMP_FUSED_CASES_9
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_8 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_8 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 10

create table NATHALIE.TMP_FUSED_CASES_10
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_9 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_9 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 11

create table NATHALIE.TMP_FUSED_CASES_11
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_10 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_10 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 12

create table NATHALIE.TMP_FUSED_CASES_12
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_11 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_11 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 13

create table NATHALIE.TMP_FUSED_CASES_13
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_12 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_12 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 14

create table NATHALIE.TMP_FUSED_CASES_14
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_13 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_13 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 15

create table NATHALIE.TMP_FUSED_CASES_15
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_14 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_14 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

--Iteration 16

create table NATHALIE.TMP_FUSED_CASES_16
as
select *
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber2
from
( --PADDING_ADDED
    select *
    from
    ( -- ROWNUMER_ADDED // order newly engineered cases above cases that are transfers and whose admit date is later
        select *
            , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber 
        from
        ( -- VALS_UPDATED // use stay_interval to decide whether to replace some FS values with their SS analogs
            select 
                case 
                    when stay_interval < 2 then ss_case_id
                    else fs_case_id
                end as case_id
                , cin_no, adm_dt
                ,   case
                        when stay_interval < 2 then ss_dis_dt
                        else fs_dis_dt
                    end as dis_dt
                , from_er, case_dx1, case_pr1, severity, aprdrg, dis_status, provider
                , case
                    when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                    else fs_paid_amt_case
                end as paid_amt_case
                , case
                    when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                    else fs_source_table
                end as source_table
                , stay_interval
            from
            ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
                select 
                    FS.case_id as fs_case_id, FS.cin_no, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.from_er, FS.case_dx1, FS.case_pr1, FS.severity
                    , FS.aprdrg, FS.dis_status, FS.provider, FS.paid_amt_case as fs_paid_amt_case, cast(FS.source_table as varchar(1)) as fs_source_table
                    , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                    -- Keep whichever is later: discharge date from FS or from SS. 
                    , case
                        when datediff(SS.dis_dt, FS.dis_dt) < 0
                            then FS.dis_dt
                            else SS.dis_dt
                        end as ss_dis_dt
                    , SS.dis_status as ss_dis_status, concat(FS.provider, ', ', SS.provider) as ss_provider, SS.paid_amt_case as ss_paid_amt_case --the paid values will be added at the next level of nesting
                    , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                    , case
                        when FS.cin_no = SS.cin_no 
                            then datediff(SS.adm_dt, FS.dis_dt)
                            else null
                        end as stay_interval
                from
                NATHALIE.TMP_FUSED_CASES_15 as FS
                inner join
                NATHALIE.TMP_FUSED_CASES_15 as SS
                ON SS.rownumber2 = FS.rownumber2 + 1
            ) AS INTERVAL_ADDED
        ) AS VALS_UPDATED
    ) AS ROWNUMER_ADDED
    union
    -- Adding a dummy row at the end of the file as padding for the next step. In the next step, the last row is sheared off
    -- when the table is joined with itself with an offset of 1 row. 
    (
        select *
        from (
            select null as case_id, '1900-01-01' as adm_dt, '1900-01-01' as dis_dt, 'ZZZZZ' as cin_no, null as case_dx1, null as case_pr1
                , null as severity, null as aprdrg, null as dis_status, null as provider, null as paid_amt_case, null as from_er, '4' as source_table
                , 99 as stay_interval, 1 as rownumber
        ) PADDING
    )
) ALL_CASES_PADDED
where rownumber = 1
;

/*
End of "loop"
If there are still more records reduced (being consolidated), then will need to run more iterations till no more reduction in total number of cases
*/

/*
Save the last iteration. Add LOS. Omit rownumber cols and any remaining padding.
*/

drop table if exists nathalie.prjrea_step1_inpatient_cases
;

create table nathalie.prjrea_step1_inpatient_cases
as
select 
    case_id
    , cin_no
    , adm_dt
    , dis_dt
    , datediff(dis_dt, adm_dt) as LOS
    , from_er
    , case_dx1
    , case_pr1
    , severity
    , aprdrg
    , dis_status
    , provider
    , paid_amt_case
    , source_table
    , stay_interval
    , rownumber2 as rownumber
    --omit one rownumber column; keep the one that helps compute readmission labels
from NATHALIE.TMP_FUSED_CASES_16
where source_table != '4' -- filter out any remaining padding
;


/*
CLEAN UP
*/

drop table if exists NATHALIE.TMP_UNIQUE_CASES;
drop table if exists NATHALIE.TMP_FUSED_CASES_1;
drop table if exists NATHALIE.TMP_FUSED_CASES_2;
drop table if exists NATHALIE.TMP_FUSED_CASES_3;
drop table if exists NATHALIE.TMP_FUSED_CASES_4;
drop table if exists NATHALIE.TMP_FUSED_CASES_5;
drop table if exists NATHALIE.TMP_FUSED_CASES_6;
drop table if exists NATHALIE.TMP_FUSED_CASES_7;
drop table if exists NATHALIE.TMP_FUSED_CASES_8;
drop table if exists NATHALIE.TMP_FUSED_CASES_9;
drop table if exists NATHALIE.TMP_FUSED_CASES_10;
drop table if exists NATHALIE.TMP_FUSED_CASES_11;
drop table if exists NATHALIE.TMP_FUSED_CASES_12;
drop table if exists NATHALIE.TMP_FUSED_CASES_13;
drop table if exists NATHALIE.TMP_FUSED_CASES_14;
drop table if exists NATHALIE.TMP_FUSED_CASES_15;
drop table if exists NATHALIE.TMP_FUSED_CASES_16;