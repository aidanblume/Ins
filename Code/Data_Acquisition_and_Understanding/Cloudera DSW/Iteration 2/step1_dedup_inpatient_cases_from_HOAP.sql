/***
Title:              step1_dedup_inpatient_cases_from_HOAP
Description:        Generates a data set of acute inpatient cases (=stays) from the HOAP.HOA "case" tables. 
                    Eliminates duplicates and merges cases that are contiguous by 1 day. 
                    Is 1st step in generating analytic data sets for readmission rate computation.
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        HOAP.HOA QNXT, CLM and ENC case tables. 
Output:             NATHALIE.prjrea_step1_inpatient_cases
Notes:              1. Inpatient cases are identified with 'where srv_cat = '01ip_a' on the sace files. 
                    This excludes more SNF inpatient stays than using substr(type_bill,1,2) in ('11','12') on the hdr files.
                    2. Unique tupple (cin_no, admi_dt) are selected with priority (1) later disc_dt, and (2) QNXT>CLM>ENC ** Note that this departs from the SAS script received in 2017
                    3. There is a potential loss of Dx, Pr and provider information when cases are deduped over cin_no and admit_dt alone. Pr and Dx info will be retrieved again later. 
***/


/*
UNIQUE_CASE_PIECES
Purpose:    To merge data from HOA's 3 case tables with priority QNXT>CLM>ENC
*/

drop table if exists NATHALIE.TMP_CASE_PIECES
;

create table NATHALIE.TMP_CASE_PIECES 
as
-- select only 1 with same (cin_no, admi_dt, dis_dt) tupple
-- add row number by cin_no partition. Will be used at next setp. 
select *
from
( --add number rows inside partitions where each partition is a unique (cin_no, admi_dt, dis_dt) tupple
    select *
    , row_number() over(partition by cin_no, adm_dt order by dis_dt desc, source_table asc, case_id desc) as rownumber
    from
    ( -- union of cases across 3 data tables: qnxt, clm, enc
        select claimid as case_id, startdate as adm_dt, enddate as dis_dt, carriermemid as cin_no
            , discharge_status as dis_status, provid as provider, 1 as source_table
        from swat.claims_universe
        where substr(provid,1,1)='H'
        and billtype2='IP-Hosp'
        union
        select case_id, adm_dt, dis_dt, cin_no, dis_status, provider
        , 2 as source_table
        from hoap.clm_case_inpsnf as C
        where srv_cat = '01ip_a'
        union
        select case_id, adm_dt, dis_dt, cin_no, dis_status
            -- ,   case 
            --         when '00' then 'Still Under Care'
            --         when '01' then 'Home'
            --         when '02' then 'Other Hospital'
            --         when '03' then 'Skilled Nursing Facility'
            --         when '04' then 'ICF'
            --         when '06' then 'Home Health'
            --         when '07' then 'AMA'
            --         when '14' then 'Hospice'
            --         when '20' then 'Exp Hospital'
            --         when '30' then 'Still In'
            --         when '40' then 'Exp Home'
            --         when '41' then 'Exp Hospital'
            --         when '42' then 'Exp, Unk Place'
            --         when '50' then 'Hospice'
            --         when '51' then 'Hospice'
            --         when '61' then 'Swing Bed'
            --         when '62' then 'Rehab-Inpatient'
            --         when '63' then 'Mdcare Ltc Hospital'
            --         when '64' then 'Mdcare Ltc Facility'
            --         when '65' then 'Psy Hospita'
            --     end as dis_status
        , provider
        , 3 as source_table
        from hoap.ENC_CASE_INPSNF as E
        where srv_cat = '01ip_a'
   ) AS PIECES
) PIECES_PARTITIONED
where rownumber =  1
;


/*
tmp_completedatepairs
--remove claims with missing startdate or enddate

check how many drops: select count(*) from nathalie.TMP_CASE_PIECES where (adm_dt is null) or (dis_dt is null);

*/

drop table if exists nathalie.tmp_completedatepairs 
;

create table nathalie.tmp_completedatepairs 
as 
select A.* 
from nathalie.TMP_CASE_PIECES as A
left anti join (select cin_no from nathalie.TMP_CASE_PIECES where (adm_dt is null) or (dis_dt is null)) as B
on A.cin_no=B.cin_no
;

/*
tmp_respaned_input
--create new pairs of start and end dates that are better time span tiles across the stay period
*/

drop table if exists nathalie.tmp_respaned_input
;

create table nathalie.tmp_respaned_input
as
select SD.cin_no, SD.adm_dt, ED.dis_dt
from 
(
    select cin_no, adm_dt, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
    from nathalie.tmp_completedatepairs
) as SD
left join
(
    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
    from nathalie.tmp_completedatepairs
) as ED
on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned
;


/*
tmp_cases
Purpose:    --find gaps >1day and define stays around them
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
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) as string)) as rnstart from nathalie.tmp_respaned_input
        ) L   
        left join
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) + 1 as string)) as rnstart from nathalie.tmp_respaned_input
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
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) + 1 as string)) as rnstart from nathalie.tmp_respaned_input
        ) L   
        left join
        (
            select *, concat(cin_no, cast(row_number() over (partition by cin_no order by adm_dt asc) as string)) as rnstart from nathalie.tmp_respaned_input
        ) R
        on L.rnstart = R.rnstart
    ) X
    where d > 1 or d is null
) E  
on S.rnlink = E.rnlink
;


/*

--attach data to cases
select * from nathalie.tmp_completedatepairs 

*/

drop table if exists nathalie.prjrea_step1_inpatient_cases
;

create table nathalie.prjrea_step1_inpatient_cases
as
select S.*, datediff(S.dis_dt, S.adm_dt) as LOS
    , 'under revision' as case_dx1, 'under revision' as case_dx2, 'under revision' as case_dx3, 'under revision' as case_dx4, 'under revision' as case_dx5
    , 'under revision' as case_dx6, 'under revision' as case_dx7, 'under revision' as case_dx8, 'under revision' as case_dx9, 'under revision' as case_dx10
    , 'under revision' as case_dx11, 'under revision' as case_dx12, 'under revision' as case_dx13, 'under revision' as case_dx14, 'under revision' as case_dx15
    , 'under revision' as case_dx16, 'under revision' as case_dx17, 'under revision' as case_dx18, 'under revision' as case_dx19, 'under revision' as case_dx20
    , 'under revision' as case_pr1, 'under revision' as case_pr2, 'under revision' as case_pr3, 'under revision' as case_pr4, 'under revision' as case_pr5
    , 'under revision' as case_pr6, 'under revision' as case_pr7, 'under revision' as case_pr8, 'under revision' as case_pr9, 'under revision' as case_pr10
    , 'under revision' as severity, 'under revision' as aprdrg, 'under revision' as paid_amt_case, 'under revision' as from_er
from
( -- info from earliest claim or HOAP case
        select Ca.case_id, Ca.cin_no, Ca.adm_dt, Ca.dis_dt, P.dis_status, P.provider, P.source_table
        , row_number() over (partition by Ca.case_id order by to_date(P.adm_dt) desc, P.source_table asc) as rndesc
        from nathalie.tmp_cases Ca 
        left join nathalie.tmp_case_pieces P 
        on Ca.cin_no=P.cin_no and P.adm_dt between Ca.adm_dt and Ca.dis_dt
) S
where S.rndesc = 1
;


/*
CLEAN UP
*/

DROP TABLE nathalie.tmp_case_pieces; DROP TABLE nathalie.tmp_cases; DROP TABLE nathalie.tmp_completedatepairs; DROP TABLE nathalie.tmp_respaned_input;