/***
Title:              step1_make_cases.sql
Description:        Generates a data set of acute inpatient cases (=stays) from the HOAP.HOA "case" tables. 
                    Eliminates duplicates and merges cases that are contiguous by 1 day. 
                    Is 1st step in generating analytic data sets for readmission rate computation.
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Sources:       swat.claims_universe
                    HOAP.HOA QNXT, CLM and ENC case tables. 
Output:             NATHALIE.prjrea_step1_inpatient_cases
Notes:              1. Inpatient cases are identified with 'where srv_cat = '01ip_a' for HOAP source. This excludes more SNF inpatient stays than using substr(type_bill,1,2) in ('11','12') on the hdr files.
                    2. Unique tupple (cin_no, admi_dt) are selected with priority (1) later disc_dt, and (2) QNXT>CLM>ENC ** Note that this departs from the SAS script received in 2017.
                    3. Provider is not used to individuate cases. The 'Provider' field is in fact 'last provider' in cases where more than one provider attendedto the member during a case. 
***/

/*
UNIQUE_CASE_PIECES
Purpose:    To merge data from swat.claimsuniverse and HOA's 3 case tables with priority Cl_U  [>QNXT]  >CLM>ENC **HOAP.QNXT has been removed. 
Plan is to replace HOAP with all tables that source hope as well as any comprehensive collection of encounters + claims. 
*/

drop table if exists NATHALIE.TMP_CASE_PIECES 
;

create table NATHALIE.TMP_CASE_PIECES
as
-- select only 1 with same (cin_no, admi_dt, dis_dt) tupple
-- add row number by cin_no partition. Will be used at next setp. 
select *
from
( --modify fields
    select case_id, cin_no
        ,   case 
                when startdate is null and enddate is not null then enddate
                else startdate
            end as adm_dt
        ,   case
                when enddate is null and startdate is not null then startdate
                when enddate < startdate then startdate
                else enddate
            end dis_dt
        , dis_status, provider, from_er, source_table
    from
    ( -- union of cases across 3 data tables: qnxt, clm, enc
        select claimid as case_id, startdate, enddate, carriermemid as cin_no
            , discharge_status as dis_status, provid as provider
            , case when admitsource='7' then 1 else 0 end as from_er
            , 1 as source_table
        from swat.claims_universe
        where substr(provid,1,1)='H'
        and billtype2='IP-Hosp'
        union
        select case_id, adm_dt as startdate, dis_dt as enddate, cin_no, dis_status, provider, case when from_er='Y' then 1 else 0 end as from_er
            , 2 as source_table
        from hoap.clm_case_inpsnf as C
        where srv_cat = '01ip_a'
        union
        select case_id, adm_dt as startdate, dis_dt as enddate, cin_no, dis_status
                -- case 
                --     when dis_status='00' then 'Still Under Care'
                --     when dis_status='01' then 'Home'
                --     when dis_status='02' then 'Other Hospital'
                --     when dis_status='03' then 'Skilled Nursing Facility'
                --     when dis_status='04' then 'ICF'
                --     when dis_status='06' then 'Home Health'
                --     when dis_status='07' then 'AMA'
                --     when dis_status='14' then 'Hospice'
                --     when dis_status='20' then 'Exp Hospital'
                --     when dis_status='30' then 'Still In'
                --     when dis_status='40' then 'Exp Home'
                --     when dis_status='41' then 'Exp Hospital'
                --     when dis_status='42' then 'Exp, Unk Place'
                --     when dis_status='50' then 'Hospice'
                --     when dis_status='51' then 'Hospice'
                --     when dis_status='61' then 'Swing Bed'
                --     when dis_status='62' then 'Rehab-Inpatient'
                --     when dis_status='63' then 'Mdcare Ltc Hospital'
                --     when dis_status='64' then 'Mdcare Ltc Facility'
                --     when dis_status='65' then 'Psy Hospita'
           --     end as dis_status
        , provider, case when from_er='Y' then 1 else 0 end as from_er
        , 3 as source_table
        from hoap.ENC_CASE_INPSNF as E
        where srv_cat = '01ip_a'
   ) AS PIECES
   where (startdate is not null or enddate is not null) -- filter out cases where both dates are null
   and provider not in (select provid from nathalie.ltach) -- filters out both null providers and ltachs. Could be a problem if null provider extends LOS. Unavoidable as Impala restricts use of subqueries to point where 'or is not null' cannot be added here. 
) PIECES_PARTITIONED
;

select count(*) from NATHALIE.TMP_CASE_PIECES_new ;
--was 1002772; is now 999911 --> loss of 2861 rows
select distinct provider from
NATHALIE.TMP_CASE_PIECES_old O
left anti join NATHALIE.TMP_CASE_PIECES_new N on O.case_id=N.case_id


/*
tmp_cases
Make cases from collections of claims & encounter files
--remove claims with missing startdate or enddate
--create new pairs of start and end dates that are better time span tiles across the stay period
--find gaps >1day and define stays around them
*/

drop table if exists nathalie.tmp_cases
;

create table nathalie.tmp_cases
as
select concat(cin_no, '_', to_date(adm_dt)) as case_id, cin_no, adm_dt, dis_dt
from
(
    select cin_no, adm_dt, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) as string)) as rnlink
    from
    (
        select L.cin_no, L.adm_dt as adm_dt, datediff(L.adm_dt, R.dis_dt) as d 
        from 
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) as string)) as rnstart 
            from
            (
                select SD.cin_no, SD.adm_dt, ED.dis_dt
                from 
                (
                    select cin_no, adm_dt, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as SD
                left join
                (
                    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as ED
                on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned            
            ) Respanned_input    
        ) L   
        left join
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by dis_dt asc) + 1 as string)) as rnstart 
            from
            (
                select SD.cin_no, SD.adm_dt, ED.dis_dt
                from 
                (
                    select cin_no, adm_dt, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as SD
                left join
                (
                    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as ED
                on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned            
            ) Respanned_input    
        ) R
        on L.rnstart = R.rnstart
    ) X
    where d > 1 or d is null
) S
left join
(
    select dis_dt,  concat(cin_no, cast(row_number() over (partition by cin_no order by dis_dt asc) as string)) as rnlink
    from
    (
        select L.cin_no, L.dis_dt as dis_dt, datediff(R.adm_dt, L.dis_dt) as d 
        from 
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by dis_dt asc) as string)) as rnend 
            from
            (
                select SD.cin_no, SD.adm_dt, ED.dis_dt
                from 
                (
                    select cin_no, adm_dt, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as SD
                left join
                (
                    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as ED
                on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned            
            ) Respanned_input    
        ) L   
        left join
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) -1 as string)) as rnend 
            from
            (
                select SD.cin_no, SD.adm_dt, ED.dis_dt
                from 
                (
                    select cin_no, adm_dt, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as SD
                left join
                (
                    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
                    from nathalie.TMP_CASE_PIECES
                    where adm_dt is not null and dis_dt is not null
                ) as ED
                on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned            
            ) Respanned_input    
        ) R
        on L.rnend = R.rnend
    ) X
    where d > 1 or d is null
) E  
on S.rnlink = E.rnlink
;

/*
--attach data to cases
- LOS
- from_ER
- provider
- cin_no
+ more under revision. 
*/

drop table if exists nathalie.prjrea_step1_inpatient_cases
;

create table nathalie.prjrea_step1_inpatient_cases
as
select CASES.case_id
    , CASES.cin_no
    , CASES.adm_dt
    , CASES.dis_dt
    , datediff(CASES.dis_dt, CASES.adm_dt) as LOS
    , MOST_RECENT.provider
    , MOST_RECENT.source_table
    , MAX_VALUE.from_er
    , 'under revision' as dis_status--P.dis_status
    , 'under revision' as severity
    , 'under revision' as aprdrg
    , 'under revision' as paid_amt_case
from nathalie.tmp_cases CASES
left join
( -- info from most recent claim or HOAP case with non null provider and highest priority source table
    select S.*
    from
    ( 
            select Ca.case_id
                , P.provider
                , P.source_table
            , row_number() over (partition by Ca.case_id order by isnull(to_date(P.adm_dt), '1900-01-01') desc, P.source_table asc) as rndesc
            from nathalie.tmp_cases Ca 
            left join nathalie.tmp_case_pieces P 
            on Ca.cin_no=P.cin_no and P.adm_dt between Ca.adm_dt and Ca.dis_dt
    ) S
    where S.rndesc = 1
) MOST_RECENT
on CASES.case_id=MOST_RECENT.case_id
left join
( -- MAX VALUE
    select Ca.case_id
        , max(from_er) as from_er
    from nathalie.tmp_cases Ca 
    left join nathalie.tmp_case_pieces P 
    on Ca.cin_no=P.cin_no and P.adm_dt between Ca.adm_dt and Ca.dis_dt
    group by Ca.case_id
) MAX_VALUE
on CASES.case_id=MAX_VALUE.case_id
;


/*
CLEAN UP
*/

DROP TABLE if exists nathalie.tmp_case_pieces; 
DROP TABLE if exists nathalie.tmp_cases; 
