/***
Title:              step4d_SNF
Description:        Adds member's SNF if the member was housed at a SNF at any time during the 90 d that precede current hospital admit date *OR* after the last 
                    inpatient discharge, whichever was more recent. If a member is admitted to several SNFs during this period, then each SNF is attached to the 
                    case that is sent to TABLEAU so that each may share in the responsibility for the readmission. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4c_PPG 
Output:             nathalie.prjrea_step4d_SNF has most recent SNF. Contains 1 row per inpatient case. This is the file that is being built for modeling purposes. 
                    -- nathalie.prjrea_tblo_readmit_SNF has several rows per inpatient case when the inpatient case was preceded by several valid SNF admits. 
                    [Note that unlike other tables meant for Tableau, here: Aggregate tables showing rate by SNF for period of N days preceding readmission 
                    are computed on the fly in Tableau rather than here]
Issues:             Alternative coding. An alternative method may be: VIA REFERRALID FIELD IN QNXT. Elected not to do because the method used seemed more direct.
                    (Steps in that alt meth: CASE to CLAIM CROSSWALK TABLE; ATTACH REFERRALID and ADMITSOURCE to DATA;
                    select claimid, referralid, admitsource from plandata.claim where [claimid matches one in the case crosswalk table]; ATTACH SNF NAME TO DATA)
***/

--

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
            when X3.days_since_preadmitSNFLTCSA_tmp is null then -1
            else X3.days_since_preadmitSNFLTCSA_tmp
        end as days_since_preadmitSNFLTCSA
    , case 
            when X3.preadmitSNFLTCSA_90dback_tmp = 1 then 1
            else 0
        end as preadmitSNFLTCSA_90dback
from
(
    select All_inp.*
        , X2.preadmitSNFLTCSA_90dback_tmp
        , X2.id_preadmitSNFLTCSA
        , X2.type_preadmitSNFLTCSA
        , X2.days_since_preadmitSNFLTCSA_tmp
        , X2.adm_dt_preadmitSNFLTCSA
        , X2.dis_dt_preadmitSNFLTCSA
    from nathalie.prjrea_step4c_PPG as All_inp
    left join
    -- ( -- select only 1 episode per SNF per case (avoid representing the same SNF multiple times per case)
    ( -- select only 1 most recent CFLTCSA stay
        select *
        from 
        ( -- Select preadmitSNFLTCSA Cases that were active during the 90 day pre-inpatient admission window  provider, provider_type
            select IP.cin_no
                , IP.adm_dt
                , IP.dis_dt
                , preadmitSNFLTCSA.adm_dt as adm_dt_preadmitSNFLTCSA
                , preadmitSNFLTCSA.dis_dt as dis_dt_preadmitSNFLTCSA
                , preadmitSNFLTCSA.provider as id_preadmitSNFLTCSA
                , preadmitSNFLTCSA.provider_type as type_preadmitSNFLTCSA
                , case 
                        when datediff(IP.adm_dt, preadmitSNFLTCSA.dis_dt) < 0 then 0
                        else datediff(IP.adm_dt, preadmitSNFLTCSA.dis_dt)
                    end as days_since_preadmitSNFLTCSA_tmp
                , 1 as preadmitSNFLTCSA_90dback_tmp
                -- , row_number() over(partition by IP.case_id order by preadmitSNFLTCSA.dis_dt desc) as rownumber --remove this line because it makes more sense to keep all SNFs in the inter-hospitalization period rather than just the most recent one.
                -- , row_number() over(partition by IP.cin_no, IP.adm_dt, IP.dis_dt, preadmitSNFLTCSA.provider order by preadmitSNFLTCSA.dis_dt desc) as rownumber -- keep the most recent valid stay for each SNF so you avoid having several rows for the same SNF when a member went in and out of it repeatedly.
                , row_number() over(partition by IP.cin_no, IP.adm_dt, IP.dis_dt order by preadmitSNFLTCSA.dis_dt desc, preadmitSNFLTCSA.adm_dt) as rownumber -- keep the most recent valid stay
            from nathalie.prjrea_step4c_PPG as IP
            left join nathalie.tmp_long_cases as preadmitSNFLTCSA
            on IP.cin_no = preadmitSNFLTCSA.cin_no
            where IP.adm_dt >= preadmitSNFLTCSA.adm_dt --keep preadmitSNFLTCSA that started before the IP admit (eliminate preadmitSNFLTCSA that began after IP)
            and IP.adm_dt < adddate(preadmitSNFLTCSA.dis_dt, 90) --keep preadmitSNFLTCSA that had yet not ended 90 day before IP admit (eliminate preadmitSNFLTCSA stays that ended long before the IP began)
            and datediff(IP.adm_dt, preadmitSNFLTCSA.dis_dt) < IP.days_since_prior_discharge --keep preadmitSNFLTCSA that were active after the last hospitalization (essentially, every hospitalization resets the clock/list of SNFs responsible for preventing the next hospitalization)
        ) X1
        where X1.rownumber = 1
    ) X2
    on All_inp.cin_no = X2.cin_no and All_inp.adm_dt = X2.adm_dt and All_inp.dis_dt = X2.dis_dt
) X3
;

/*
ADD SNF NAMES (FOR SNF THAT PRECEDED KNOWN INP)
*/

drop table if exists nathalie.tmp2
;

create table nathalie.tmp2
as
select A.*
    , PROVNAME_REF.preadmitSNFLTCSAname
from nathalie.tmp as A
left join
(
    select cin_no, adm_dt, fullname as preadmitSNFLTCSAname
    from
    (
        select cin_no, adm_dt, fullname, row_number() over(partition by cin_no, adm_dt order by source asc) as rn
        from
        (
            --select names primarily from QNXT's provider table
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, B.fullname, 1 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_preadmitSNFLTCSA = B.provid
            where B.fullname is not null
            and A.preadmitSNFLTCSA_90dback = 1
            --if names not found in QNXT provider table then do the following:
            union
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, B.fullname, 2 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_preadmitSNFLTCSA = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value because you want to preserve SNF info as much as possible
            and B.fullname is not null
            and A.preadmitSNFLTCSA_90dback = 1
            union
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, B.fullname, 3 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_preadmitSNFLTCSA = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null
            and A.preadmitSNFLTCSA_90dback = 1
            union
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, B.fullname, 4 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_preadmitSNFLTCSA = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            and A.preadmitSNFLTCSA_90dback = 1
            union
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, B.fullname, 5 as source -- there is some kind of match to a fullname
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.id_preadmitSNFLTCSA = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            and A.preadmitSNFLTCSA_90dback = 1
            --Below ensures that all name_preadmitSNFLTCSA cases are kept, whether or not a name was found
            union
            select A.cin_no, A.adm_dt, A.id_preadmitSNFLTCSA, A.id_preadmitSNFLTCSA as fullname, 6 as source -- there is no match to a fullname, but A.name_preadmitSNFLTCSA may not be null
            from nathalie.tmp as A
            where A.preadmitSNFLTCSA_90dback = 1
            and A.id_preadmitSNFLTCSA is not null
            union
            select A.cin_no, A.adm_dt, id_preadmitSNFLTCSA, null as fullname, 6 as source -- A.name_preadmitSNFLTCSA is null
            from nathalie.tmp as A
            where A.preadmitSNFLTCSA_90dback = 1
            and A.id_preadmitSNFLTCSA is null
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

drop table if exists nathalie.tmp_traffic
;

create table nathalie.tmp_traffic
as
-- select provider, fullname, yrmo, count(distinct cin_no) as admitCount
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
-- group by provider, fullname, yrmo
;

/*
ADD COUNT OF UNIQUE CIN_NO ADMITS AT SNF THAT MONTH
*/

drop table if exists nathalie.tmp3
;

create table nathalie.tmp3
as
select A.*, B.admitcount as SNFLTCSA_admitsthismonth
from
(
    select *
        , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
    from nathalie.tmp2
) A
left join nathalie.tmp_traffic B
on A.preadmitsnfltcsaname = B.fullname
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

drop table if exists prjrea_step4d_SNF
;

create table prjrea_step4d_SNF
as
select A.*
    , B.preadmitsnfltcsaname 
    , B.type_preadmitSNFLTCSA
    , B.days_since_preadmitsnfltcsa --recall that -1 means no snf
    , case 
            when B.tmpval = 1 then 1
            else 0
        end as snfltcsa_90dback
    , case 
            when (B.days_since_preadmitsnfltcsa > -1 and B.days_since_preadmitsnfltcsa <= 1) then 1
            else 0
        end as snfltcsa_1dback
    , case 
            when (B.days_since_preadmitsnfltcsa > -1 and B.days_since_preadmitsnfltcsa <= 3) then 1
            else 0
        end as snfltcsa_3dback
    , case 
            when (B.days_since_preadmitsnfltcsa > -1 and B.days_since_preadmitsnfltcsa <= 7) then 1
            else 0
        end as snfltcsa_7dback
    , case 
            when (B.days_since_preadmitsnfltcsa > -1 and B.days_since_preadmitsnfltcsa <= 14) then 1
            else 0
        end as snfltcsa_14dback
    , B.snfltcsa_admitsthismonth
from nathalie.prjrea_step4c_PPG as A
left join 
(
    select *
        , 1 as tmpval
    from 
    (
        select cin_no, adm_dt, preadmitsnfltcsaname, type_preadmitSNFLTCSA, days_since_preadmitsnfltcsa, snfltcsa_admitsthismonth, row_number() over(partition by cin_no, adm_dt order by days_since_preadmitsnfltcsa asc) as rn
        from nathalie.tmp3 
        where preadmitsnfltcsa_90dback = 1
    ) S
    where rn = 1
) as B
on A.cin_no = B.cin_no 
and A.adm_dt = B.adm_dt
;

-- select preadmitsnfltcsaname, type_preadmitSNFLTCSA, count(*) from prjrea_step4d_SNF
--  where preadmitsnfltcsaname like '%KINDRED%' or preadmitsnfltcsaname like '%Barlow%' or preadmitsnfltcsaname like '%Promise %'
--  group by preadmitsnfltcsaname, type_preadmitSNFLTCSA


set max_row_size=1mb; 

-- select type_preadmitSNFLTCSA from prjrea_step4d_SNF

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

drop table if exists nathalie.tmp_traffic
;

drop table if exists nathalie.tmp_raw_input
;

drop table if exists nathalie.tmp_completedatepairs 
;

drop table if exists nathalie.tmp_respaned_input
;

drop table if exists nathalie.tmp_cases
;

drop table if exists nathalie.tmp_long_cases
;
