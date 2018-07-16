/***
Title:              step4e_postDischSNF
Description:        Adds member's SNF if the member was housed at a SNF at any time during the 90 d that FOLLOW current hospital discharge date *OR* between hospital discharge 
                    and the next inpatient admit, whichever comes first. If a member is admitted to several SNFs during this period, then each SNF is attached to the 
                    case that is sent to TABLEAU so that each may share in the responsibility for the readmission. 
                    THE RESULTING FILE IS SENT TO TABLEAU ONLY
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4d_SNF 
Output:             FOR TABLEAU: 
                    nathalie.prjrea_tblo_postDischSNF has several rows per inpatient case when the inpatient case was preceded by several valid SNF admits. 
Issues:             In teh future, an analytic file may be generated where acute admits are still unique rows. The post discharge SNF info may be reduced to
                    a column indicating the date of the 1st SNF admit post discharge.
***/

/*
FOR INP, ATTACH SNF THAT PRECEDED (IF ANY)
Left JOIN TO EACH ADMIT CASE each SNF admit that occurred between [90 days prior to readmit OR the day of index discharge, whichever comes later] and readmit to hospital.
The end result will be a file with more rows than the original file, where certain cin_no+adm_date will now appear on more than 1 row. 
*/

drop table if exists nathalie.tmp
;

create table nathalie.tmp
as
--For each inpatient case, attach to it (if any) the most recent SNF case that happened within the 90 day window preceding admission.
select X3.*
    , case 
            when X3.days_until_SNF_tmp is null then -1
            else X3.days_until_SNF_tmp
        end as days_until_SNF
    , case 
            when X3.snf_90dfwd_tmp = 1 then 1
            else 0
        end as snf_90dfwd
from
(
    select All_inp.*
        , X2.snf_90dfwd_tmp
        , X2.SNF
        , X2.days_until_SNF_tmp
        , X2.adm_dt_SNF
        , X2.dis_dt_SNF
    from prjrea_step4d_SNF as All_inp
    left join
    ( -- select only 1 episode per SNF per case (avoid representing the same SNF multiple times per case)
        select *
        from   
        (
            select cin_no, adm_dt, dis_dt, snf_90dfwd_tmp, days_until_SNF_tmp, adm_dt_SNF, dis_dt_SNF, SNF 
                -- , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_SNF_tmp asc, dis_dt_SNF desc) as rownumber -- keep the earliest and longest valid stay for any SNF
                , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_SNF_tmp desc, dis_dt_SNF desc) as rownumber -- keep the LATER (and that being equal, the longest) valid stay for any SNF. Reason: Responsibiity for readmission lies with the last SNF to have custody of the member. 
            from 
            ( -- Select SNF Cases that were active during the 90 day pre-inpatient admission window
                select IP.cin_no
                    , IP.adm_dt
                    , IP.dis_dt
                    , SNF.adm_dt as adm_dt_SNF
                    , SNF.dis_dt as dis_dt_SNF
                    , SNF.provider as SNF
                    , case 
                            when datediff(SNF.adm_dt, IP.dis_dt) < 0 then 0
                            else datediff(SNF.adm_dt, IP.dis_dt)
                        end as days_until_SNF_tmp
                    , 1 as snf_90dfwd_tmp
                from prjrea_step4d_SNF as IP
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
                            from hoap.QNXT_CASE_INPSNF
                            where srv_cat = '04snf'
                            union
                            select case_id, cin_no, adm_dt, dis_dt, provider
                            , 2 as source_table
                            from hoap.clm_case_inpsnf
                            where srv_cat = '04snf'
                            union
                            select case_id, cin_no, adm_dt, dis_dt, provider
                            , 3 as source_table
                            from hoap.ENC_CASE_INPSNF
                            where srv_cat = '04snf'
                       ) AS ALL_CASES
                    ) ALL_CASES_PARTITIONED
                ) as SNF
                on IP.cin_no = SNF.cin_no
                where days_add(IP.dis_dt, IP.days_until_next_admit) >= SNF.adm_dt --keep SNF that started before the next IP admit (eliminate SNF that began after next IP admit)
                and IP.dis_dt < SNF.dis_dt --keep SNF that existed after IP discharge (eliminate SNF stays that ended before IP discharge)
            ) X
        ) X1
        where rownumber = 1
    ) X2
    on All_inp.cin_no = X2.cin_no and All_inp.adm_dt = X2.adm_dt and All_inp.dis_dt = X2.dis_dt
) X3
;

/*
ADD SNF NAMES (FOR SNF THAT FOLLOWED KNOWN INP)
*/

drop table if exists nathalie.tmp2
;

create table nathalie.tmp2
as
select A.*
    , PROVNAME_REF.postdischarge_SNFname
from nathalie.tmp as A
left join
(
    select cin_no, adm_dt, fullname as postdischarge_SNFname
    from
    (
        select cin_no, adm_dt, fullname, row_number() over(partition by cin_no, adm_dt order by source asc) as rn
        from
        (
            --select names primarily from QNXT's provider table
            select A.cin_no, A.adm_dt, A.SNF, B.fullname, 1 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.SNF = B.provid
            where B.fullname is not null
            and A.snf_90dfwd = 1
            --if names not found in QNXT provider table then do the following:
            union
            select A.cin_no, A.adm_dt, A.SNF, B.fullname, 2 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.SNF = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value because you want to preserve SNF info as much as possible
            and B.fullname is not null
            and A.snf_90dfwd = 1
            union
            select A.cin_no, A.adm_dt, A.SNF, B.fullname, 3 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.SNF = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null
            and A.snf_90dfwd = 1
            union
            select A.cin_no, A.adm_dt, A.SNF, B.fullname, 4 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.SNF = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            and A.snf_90dfwd = 1
            union
            select A.cin_no, A.adm_dt, A.SNF, B.fullname, 5 as source -- there is some kind of match to a fullname
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.SNF = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            and A.snf_90dfwd = 1
            --Below ensures that all SNF cases are kept, whether or not a name was found
            union
            select A.cin_no, A.adm_dt, A.SNF, A.SNF as fullname, 6 as source -- there is no match to a fullname, but A.SNF may not be null
            from nathalie.tmp as A
            where A.snf_90dfwd = 1
            and A.SNF is not null
            union
            select A.cin_no, A.adm_dt, 'UNK' as SNF, 'UNK' as fullname, 6 as source -- there is no match to a fullname, but A.SNF may not be null
            from nathalie.tmp as A
            where A.snf_90dfwd = 1
            and A.SNF is null
        ) S
        --where fullname is not null --Eliminate because: for some case_ids, there is no SNF identifier, just knowledge of a SNF. We want to preserve traces of these cases.
    ) S2
    where rn = 1
) PROVNAME_REF
on A.cin_no = PROVNAME_REF.cin_no
and A.adm_dt = PROVNAME_REF.adm_dt
;


/*
COMPUTE THE NUMBER OF ADMITS AT EACH SNF EACH MONTH (REGARDLESS OF INP)
*/

drop table if exists nathalie.SNFtraffic
;

create table nathalie.SNFtraffic
as
select postdischarge_SNFname, yrmo, count(distinct cin_no) as admitCount
from   
(
    select *
        , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
        , row_number() over(partition by cin_no, adm_dt order by source asc) as rn
    from   
    (
        select A.*, B.fullname as postdischarge_SNFname, 1 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
        left join plandata.provider as B
        on A.SNF = B.provid
        where B.fullname is not null
        union
        select A.*, B.fullname as postdischarge_SNFname, 2 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
        left join plandata.provider as B
        on A.SNF = B.fedid
        where B.provtype in ('88') 
        and B.fullname is not null
        union
        select A.*, B.fullname as postdischarge_SNFname, 3 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
        left join plandata.provider as B
        on A.SNF = B.fedid
        where B.provtype in ('16', '70')
        and B.fullname is not null
        union
        select A.*, B.fullname as postdischarge_SNFname, 4 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
        left join plandata.provider as B
        on A.SNF = B.fedid
        where B.provtype in ('15', '46')
        and B.fullname is not null
        union
        select A.*, B.fullname as postdischarge_SNFname, 5 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
        left join plandata.provider as B
        on A.SNF = B.fedid
        where B.provtype not in ('88', '70', '16', '15', '46')
        and B.fullname is not null
        --Below ensures that all SNF cases are kept, whether or not a name was found
        union
        select A.*, A.SNF as postdischarge_SNFname, 6 as source
        from 
        (
            select *
            from
            ( 
                select *
                , row_number() over(partition by cin_no, adm_dt order by source_table asc, SNF desc) as rownumber
                from
                ( -- union of cases across 3 data tables: qnxt, clm, enc
                    select cin_no, adm_dt, provider as SNF
                    , 1 as source_table
                    from hoap.QNXT_CASE_INPSNF
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 2 as source_table
                    from hoap.clm_case_inpsnf
                    where srv_cat = '04snf'
                    union
                    select cin_no, adm_dt, provider as SNF
                    , 3 as source_table
                    from hoap.ENC_CASE_INPSNF
                    where srv_cat = '04snf'
               ) AS ALL_CASES
                order by cin_no, adm_dt
            ) ALL_CASES_PARTITIONED
            where rownumber =  1
        ) A
    ) X1
) X2
where rn = 1
group by postdischarge_SNFname, yrmo
;

/*
ADD COUNT OF UNIQUE CIN_NO ADMITS AT SNF THAT MONTH
*/

drop table if exists nathalie.tmp3
;

create table nathalie.tmp3
as
select A.*, B.admitcount as postdischarge_SNF_admitsthismonth
from
(
    select *
        , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
    from nathalie.tmp2
) A
left join SNFtraffic B
on A.postdischarge_SNFname = B.postdischarge_SNFname
and A.yrmo = B.yrmo
;

/*
GENERATE ANALYTIC FILE BY REDUCING TO 1 ROW PER HOSPITAL ADMIT

nathalie.prjrea_step4d_SNF has most recent SNF. Contains 1 row per inpatient case. This is the file that is being built for modeling purposes. 

To reduce the file, rather than select the name of the most recent SNF, drop SNF names altogether and compute the existence of a SNF 
(1) at all in 90 d or after index discharge, (2) within 1 day of admission, (3) within 3 days of admission, (4) within 7 days of admission,
(5) within 14 days of admission.
*/

set max_row_size=7mb;

drop table if exists prjrea_step4e_postdischargeSNF
;

create table prjrea_step4e_postdischargeSNF
as
select A.*
    , B.postdischarge_snfname
    , B.days_until_SNF --recall that -1 means no snf
    , case 
            when B.tmpval = 1 then 1
            else 0
        end as snf_90dfwd
    , case 
            when (B.days_until_SNF > -1 and B.days_until_SNF <= 1) then 1
            else 0
        end as snf_1dfwd
    , case 
            when (B.days_until_SNF > -1 and B.days_until_SNF <= 3) then 1
            else 0
        end as snf_3dfwd
    , case 
            when (B.days_until_SNF > -1 and B.days_until_SNF <= 7) then 1
            else 0
        end as snf_7dfwd
    , case 
            when (B.days_until_SNF > -1 and B.days_until_SNF <= 14) then 1
            else 0
        end as snf_14dfwd
    , B.postdischarge_SNF_admitsthismonth
from nathalie.prjrea_step4d_SNF as A
left join 
(
    select *
        , 1 as tmpval
    from 
    (
        select cin_no, adm_dt, postdischarge_snfname, days_until_SNF, postdischarge_SNF_admitsthismonth
        , row_number() over(partition by cin_no, adm_dt order by days_until_snf asc) as rn
        from nathalie.tmp3 
        where snf_90dfwd = 1
    ) S
    where rn = 1
) as B
on A.cin_no = B.cin_no 
and A.adm_dt = B.adm_dt
;

set max_row_size=1mb; 

/*
GENERATE FILE FOR TABLEAU BY KEEPING EXTRA ROWS AND FILTERING OUT UNNECESSARY FIELDS

nathalie.prjrea_tblo_readmit_SNF has several rows per inpatient case when the inpatient case was preceded by several valid SNF admits. 

Not all fields are required

[Note that unlike other tables meant for Tableau, here: Aggregate tables showing rate by SNF for period of N days preceding readmission are computed on the fly in Tableau rather than here]



-- moot because in temp, only most recent SNF stay is kept.
-- worth doing eventually because some SNFs may play hot potato, transfering members they know are headed toward admission to other SNFs. 
-- but at the moment the tableau dashboard would become complex. Need to bring up as part of design planning.

*/

-- drop table if exists nathalie.prjrea_tblo_readmit_SNF
-- ;

-- create table nathalie.prjrea_tblo_readmit_SNF
-- as
-- select 
--     case_id, cin_no, adm_dt, dis_dt
--     , SNFname, SNF_admitsthismonth, days_since_SNF, adm_dt_SNF, dis_dt_SNF
--     , is_a_30d_death, is_a_30d_readmit, is_a_90d_readmit
--     , product_code, product_name, segment
-- from nathalie.tmp3
-- where snf_90dback = 1 -- not necessary but emphasizes that only admits preceded by a SNF since last discharge or within 90 days of admit are included
-- and dies_before_discharge = 0
-- ;

/*
CLEAN UP
*/

drop table if exists nathalie.tmp
;

drop table if exists nathalie.tmp2
;

drop table if exists nathalie.tmp3
;

drop table if exists SNFtraffic
;