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
Create SNF, LTC and SA cases.
*/

drop table if exists nathalie.tmp_raw_input
;

-- select all claims and encounters for SNF-Inpatient with revenue codes for subacute, snf and ltc
create table nathalie.tmp_raw_input
as

select claimid as case_id, carriermemid as cin_no, startdate as adm_dt, enddate as dis_dt, provid as provider
    , case when ltc_claim = 'yes' then 'ltc' when snf_claim = 'yes' then 'snf' when suba_claim = 'yes' then 'subacute' end as provider_type
    , 1 as source
from swat.claims_universe
where (ltc_claim = 'yes' or snf_claim = 'yes' or suba_claim = 'yes')
union

select case_id, cin_no, adm_dt, dis_dt, provider, 
    case 
        when lpad(rev_cd, 4, '0') in ('0022', '0191', '0192', '0193', '0194') then 'snf'
        when lpad(rev_cd, 4, '0') = '0199' then 'subacute'
        when lpad(rev_cd, 4, '0') = '0160' then 'ltc'
    end as provider_type
    , 2 as source
from
(
    select C.case_id, H.cin_no, adm_dt, dis_dt, H.provider, type_bill, rev_cd
    FROM hoap.qnxt_case_inpsnf as C
    left join hoap.qnxt_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.qnxt_detail_inpsnf as D
    on H.cl_id = D.cl_id
    union
    select C.case_id, H.cin_no, adm_dt, dis_dt, H.provider, type_bill, rev_cd
    from hoap.clm_case_inpsnf as C
    left join hoap.clm_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.clm_detail_inpsnf as D
    on H.cl_id = D.cl_id
    union
    select C.case_id, H.cin_no, adm_dt, dis_dt, H.provider, type_bill, rev_cd
    from hoap.ENC_CASE_INPSNF as C
    left join hoap.enc_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.enc_detail_inpsnf as D
    on H.cl_id = D.cl_id
) S
where substr(type_bill, 1, 2) in ('21', '22')
and lpad(rev_cd, 4, '0') in ('0022', '0160', '0191', '0192', '0193', '0194', '0199')

union --Identify the LTACHs from a list provided by PNM 
select case_id, cin_no, adm_dt, dis_dt, provider
    , 'ltach' as provider_type
    , 3 as source
from 
(
    select distinct provid 
    from plandata.provider 
    where 
        upper(fullname) like '%KINDRED HOSPITAL - BALDWIN PARK%' 
        or upper(fullname) like '%KINDRED HOSPITAL - LOS ANGELES%' 
        or upper(fullname) like '%KINDRED HOSPITAL - SAN GABRIEL%' 
        or upper(fullname) like '%KINDRED HOSPITAL - LA MIRADA%' 
        or upper(fullname) like '%KINDRED HOSPITAL - SOUTH BAY%' 
        or upper(fullname) like '%BARLOW RESPIRATORY%' 
        or upper(fullname) like '%PROMISE HOSP%SUBURBAN%'
) X
left join
(
    -- H0000203 has no nip and the fullname is essentially the same as fullname for H0000621 (Promise)
    select C.case_id, H.cin_no, adm_dt, dis_dt, case when H.provider = 'H0000203' then 'H0000621' else H.provider end as provider, type_bill, rev_cd
    FROM hoap.qnxt_case_inpsnf as C
    left join hoap.qnxt_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.qnxt_detail_inpsnf as D
    on H.cl_id = D.cl_id
    union
    select C.case_id, H.cin_no, adm_dt, dis_dt, case when H.provider = 'H0000203' then 'H0000621' else H.provider end as provider, type_bill, rev_cd
    from hoap.clm_case_inpsnf as C
    left join hoap.clm_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.clm_detail_inpsnf as D
    on H.cl_id = D.cl_id
    union
    select C.case_id, H.cin_no, adm_dt, dis_dt, case when H.provider = 'H0000203' then 'H0000621' else H.provider end as provider, type_bill, rev_cd
    from hoap.ENC_CASE_INPSNF as C
    left join hoap.enc_hdr_inpsnf as H
    on C.case_id = H.case_id
    left join hoap.enc_detail_inpsnf as D
    on H.cl_id = D.cl_id
) Y
on X.provid=Y.provider 
where provider is not null
;



--remove claims with missing adm_dt or dis_dt

drop table if exists nathalie.tmp_completedatepairs 
;

create table nathalie.tmp_completedatepairs 
as 
select A.* 
from nathalie.tmp_raw_input as A
left anti join (select cin_no from nathalie.tmp_raw_input where (adm_dt is null) or (dis_dt is null)) as B
on A.cin_no=B.cin_no
;

--create new pairs of start and end dates that are better time span tiles across the stay period

drop table if exists nathalie.tmp_respaned_input
;

create table nathalie.tmp_respaned_input
as
select SD.cin_no, SD.provider, SD.provider_type, SD.adm_dt, ED.dis_dt
from 
(
    select cin_no, adm_dt, provider, provider_type, row_number() OVER (PARTITION BY cin_no ORDER BY adm_dt asc) as rnsd
    from nathalie.tmp_completedatepairs
) as SD
left join
(
    select cin_no, dis_dt, row_number() OVER (PARTITION BY cin_no ORDER BY dis_dt asc) as rned
    from nathalie.tmp_completedatepairs
) as ED
on SD.cin_no=ED.cin_no and SD.rnsd=ED.rned
;


--Group claims into cases

drop table if exists nathalie.tmp_cases
;

create table nathalie.tmp_cases
as
select cin_no, provider, provider_type, adm_dt, dis_dt
from
(
    select cin_no, provider, provider_type, adm_dt, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) as string)) as rnlink
    from
    (
        select L.cin_no, L.provider, L.provider_type, L.adm_dt as adm_dt, datediff(L.adm_dt, R.dis_dt) as d 
        from 
        (
            select *, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) as string)) as rnstart from nathalie.tmp_respaned_input
        ) L   
        left join
        (
            select *, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) + 1 as string)) as rnstart from nathalie.tmp_respaned_input
        ) R
        on L.rnstart = R.rnstart
    ) X
    where d > 1 or d is null
) S
left join
(
    select dis_dt,  concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by dis_dt asc) as string)) as rnlink
    from
    (
        select L.cin_no, L.provider, L.provider_type, L.dis_dt as dis_dt, datediff(R.adm_dt, L.dis_dt) as d 
        from 
        (
            select *, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) + 1 as string)) as rnstart from nathalie.tmp_respaned_input
        ) L   
        left join
        (
            select *, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) as string)) as rnstart from nathalie.tmp_respaned_input
        ) R
        on L.rnstart = R.rnstart
    ) X
    where d > 1 or d is null
) E  
on S.rnlink = E.rnlink
;

--Keep cases longer than 1 day

drop table if exists nathalie.tmp_long_cases
;

create table nathalie.tmp_long_cases
as
select * from nathalie.tmp_cases 
where datediff(dis_dt, adm_dt) > 1
;

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
            when X3.days_until_SNFLTCSA_tmp is null then -1
            else X3.days_until_SNFLTCSA_tmp
        end as days_until_SNFLTCSA
    , case 
            when X3.snfltcsa_90dfwd_tmp = 1 then 1
            else 0
        end as snfltcsa_90dfwd
from
(
    select All_inp.*
        , X2.snfltcsa_90dfwd_tmp
        , X2.id_postdischargeSNFLTCSA
        , X2.type_postdischargeSNFLTCSA
        , X2.days_until_SNFLTCSA_tmp
        , X2.adm_dt_postdischargeSNFLTCSA
        , X2.dis_dt_postdischargeSNFLTCSA
    from prjrea_step4d_SNF as All_inp
    left join
    ( -- select only 1 episode per SNF per case (avoid representing the same SNF multiple times per case)
        select *
        from   
        (
            select cin_no, adm_dt, dis_dt
                , snfltcsa_90dfwd_tmp, days_until_SNFLTCSA_tmp, adm_dt_postdischargeSNFLTCSA, dis_dt_postdischargeSNFLTCSA, id_postdischargeSNFLTCSA, type_postdischargeSNFLTCSA
                -- , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_SNFLTCSA_tmp asc, dis_dt_SNF desc) as rownumber -- keep the earliest and longest valid stay for any SNF
                , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_terminalmoment desc, days_until_SNFLTCSA_tmp desc, dis_dt_postdischargeSNFLTCSA desc) as rownumber -- keep the LAST BEFORE READMIT (and that being equal, the longest) valid stay for any SNF. If no readmit, select the last in 90d period. Reason: Responsibiity for readmission lies with the last SNF to have custody of the member. 
            from 
            (
                select *
                    , 1 as snfltcsa_90dfwd_tmp
                    , case 
                            when (90 - days_until_next_admit) >= 0 then (days_until_next_admit - days_until_SNFLTCSA_tmp) -- if readmitted to hospital before 90d are up then take num days between snf discharge and readmission 
                            else (90 - days_until_SNFLTCSA_tmp) --if not readmitted in 90d take num days between snf admit and 90d with min being 0 
                        end as days_until_terminalmoment --recency of SNFLTCSA by time hospital readmission happens or 90 days post hospitalization are up, whichever comes first
                from
                ( -- Select SNF Cases that were active during the 90 day pre-inpatient admission window
                    select IP.cin_no
                        , IP.adm_dt
                        , IP.dis_dt
                        , IP.days_until_next_admit
                        , SNFLTCSA.adm_dt as adm_dt_postdischargeSNFLTCSA
                        , SNFLTCSA.dis_dt as dis_dt_postdischargeSNFLTCSA
                        , SNFLTCSA.provider as id_postdischargeSNFLTCSA
                        , SNFLTCSA.provider_type as type_postdischargeSNFLTCSA
                        , case 
                                when datediff(SNFLTCSA.adm_dt, IP.dis_dt) < 0 then 0
                                else datediff(SNFLTCSA.adm_dt, IP.dis_dt)
                            end as days_until_SNFLTCSA_tmp
                    from prjrea_step4d_SNF as IP
                    right join --right not left is required in order to limit set to 'has SNF within 90 d'
                    nathalie.tmp_long_cases as SNFLTCSA
                    on IP.cin_no = SNFLTCSA.cin_no
                    where days_add(IP.dis_dt, IP.days_until_next_admit) >= SNFLTCSA.adm_dt --keep SNFLTCSA that started before the next IP admit (eliminate SNF that began after next IP admit)
                    and IP.dis_dt < SNFLTCSA.dis_dt --keep SNFSNFLTCSA that existed after IP discharge (eliminate SNFLTCSA stays that ended before IP discharge)
                ) X
                where days_until_SNFLTCSA_tmp <= 90 and days_until_SNFLTCSA_tmp >= 0
            )X0
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
    , PROVNAME_REF.postdischarge_SNFLTCSAname
from nathalie.tmp as A
left join
(
    select cin_no, adm_dt, fullname as postdischarge_SNFLTCSAname
    from
    (
        select cin_no, adm_dt, fullname, row_number() over(partition by cin_no, adm_dt order by source asc) as rn
        from
        ( 
            --select names primarily from QNXT's provider table
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, B.fullname, 1 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_postdischargeSNFLTCSA = B.provid
            where B.fullname is not null
            and A.snfltcsa_90dfwd_tmp = 1
            --if names not found in QNXT provider table then do the following:
            union
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, B.fullname, 2 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_postdischargeSNFLTCSA = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value because you want to preserve SNF info as much as possible
            and B.fullname is not null
            and A.snfltcsa_90dfwd_tmp = 1
            union
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, B.fullname, 3 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_postdischargeSNFLTCSA = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null
            and A.snfltcsa_90dfwd_tmp = 1
            union
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, B.fullname, 4 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_postdischargeSNFLTCSA = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            and A.snfltcsa_90dfwd_tmp = 1
            union
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, B.fullname, 5 as source -- there is some kind of match to a fullname
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_postdischargeSNFLTCSA = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            and A.snfltcsa_90dfwd_tmp = 1
            --Below ensures that all SNF cases are kept, whether or not a name was found
            union
            select A.cin_no, A.adm_dt, A.id_postdischargeSNFLTCSA, A.id_postdischargeSNFLTCSA as fullname, 6 as source -- there is no match to a fullname, but A.SNF may not be null
            from nathalie.tmp as A
            where A.snfltcsa_90dfwd_tmp = 1
            and A.id_postdischargeSNFLTCSA is not null
            union
            select A.cin_no, A.adm_dt, null as id_postdischargeSNFLTCSA, null as fullname, 6 as source -- A.id_postdischargeSNFLTCSA is null
            from nathalie.tmp as A
            where A.snfltcsa_90dfwd_tmp = 1
            and A.id_postdischargeSNFLTCSA is null
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

drop table if exists nathalie.tmp_traffic_monthly
;

create table nathalie.tmp_traffic_monthly
as
select distinct *
from 
(
    select fullname, yrmo, count(distinct cin_no) as admitCount
    from   
    (
        select *
            , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
            , row_number() over(partition by cin_no, adm_dt order by source asc) as rn
        from   
        (
            select A.*, B.fullname, 1 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.provid
            where B.fullname is not null
            union
            select A.*, B.fullname, 2 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('88') 
            and B.fullname is not null
            union
            select A.*, B.fullname, 3 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('16', '70')
            and B.fullname is not null
            union
            select A.*, B.fullname, 4 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            union
            select A.*, B.fullname, 5 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            union
            --Below ensures that all SNF cases are kept, whether or not a name was found
            select A.*, A.provider as fullname, 6 as source
            from nathalie.tmp_long_cases as A
        ) X1
    ) X2
    where rn = 1
    group by fullname, yrmo
) X3
where fullname is not null
;

drop table if exists nathalie.tmp_traffic_wholeperiod
;

create table nathalie.tmp_traffic_wholeperiod
as
select distinct *
from 
(
    select fullname, count(distinct cin_no) as admitCount
    from   
    (
        select *
            , row_number() over(partition by cin_no, adm_dt order by source asc) as rn
        from   
        (
            select A.*, B.fullname, 1 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.provid
            where B.fullname is not null
            union
            select A.*, B.fullname, 2 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('88') 
            and B.fullname is not null
            union
            select A.*, B.fullname, 3 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('16', '70')
            and B.fullname is not null
            union
            select A.*, B.fullname, 4 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            union
            select A.*, B.fullname, 5 as source
            from nathalie.tmp_long_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            union
            --Below ensures that all SNF cases are kept, whether or not a name was found
            select A.*, A.provider as fullname, 6 as source
            from nathalie.tmp_long_cases as A
        ) X1
    ) X2
    where rn = 1
    group by fullname
) X3
where fullname is not null
;

/*
ADD COUNT OF UNIQUE CIN_NO ADMITS AT SNF THAT MONTH and over the whole period
*/

drop table if exists nathalie.tmp4
;

create table nathalie.tmp4
as
select A.*, B.admitcount as uniquemember_postdischargeSNFLTCSA_admitsthismonth, C.admitcount as uniquemember_postdischargeSNFLTCSA_admitsthisPeriod
from
(
    select *
        , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
    from nathalie.tmp2
) A
left join nathalie.tmp_traffic_monthly B
on A.postdischarge_SNFLTCSAname = B.fullname and A.yrmo = B.yrmo
left join nathalie.tmp_traffic_wholeperiod C
on A.postdischarge_SNFLTCSAname = C.fullname
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
    , B.postdischarge_snfltcsaname
    , B.type_postdischargeSNFLTCSA
    , B.days_until_snfltcsa --recall that -1 means no snf
    , case 
            when B.tmpval = 1 then 1
            else 0
        end as snfltcsa_90dfwd
    , case 
            when (B.days_until_snfltcsa > -1 and B.days_until_snfltcsa <= 1) then 1
            else 0
        end as snf_1dfwd
    , case 
            when (B.days_until_snfltcsa > -1 and B.days_until_snfltcsa <= 3) then 1
            else 0
        end as snf_3dfwd
    , case 
            when (B.days_until_snfltcsa > -1 and B.days_until_snfltcsa <= 7) then 1
            else 0
        end as snf_7dfwd
    , case 
            when (B.days_until_snfltcsa > -1 and B.days_until_snfltcsa <= 14) then 1
            else 0
        end as snf_14dfwd
    , B.uniquemember_postdischargesnfltcsa_admitsthismonth
    , B.uniquemember_postdischargesnfltcsa_admitsthisperiod
from nathalie.prjrea_step4d_SNF as A
left join 
(
    select *
        , 1 as tmpval
    from 
    (
        select cin_no, adm_dt, postdischarge_snfltcsaname, type_postdischargeSNFLTCSA, days_until_snfltcsa, uniquemember_postdischargesnfltcsa_admitsthismonth, uniquemember_postdischargesnfltcsa_admitsthisperiod 
        , row_number() over(partition by cin_no, adm_dt order by days_until_snfltcsa asc) as rn
        from nathalie.tmp4 
        where snfltcsa_90dfwd = 1
    ) S
    where rn = 1
) as B
on A.cin_no = B.cin_no 
and A.adm_dt = B.adm_dt
;

set max_row_size=1mb; 



/*
CLEAN UP
*/

drop table if exists nathalie.tmp
;

drop table if exists nathalie.tmp2
;

drop table if exists nathalie.tmp3
;

drop table if exists nathalie.tmp4
;

drop table if exists SNFtraffic1
;

drop table if exists SNFtraffic2
;