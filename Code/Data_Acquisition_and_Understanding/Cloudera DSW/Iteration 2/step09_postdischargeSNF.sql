/***
Title:              step9_postDischSNF
Description:        Adds member's SNF if the member was housed at a SNF at any time during the 90 d that FOLLOW current hospital discharge date *OR* between hospital discharge 
                    and the next inpatient admit, whichever comes first. If a member is admitted to several SNFs during this period, then the last admit is shown. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step8_outpatient
Output:             nathalie.prjrea_step9_postDischSNF  
Issues:             In the future, an analytic file may be generated where acute admits are still unique rows. The post discharge SNF info may be reduced to
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
select case_id, cin_no, adm_dt, dis_dt, provider
    , case when ltach = 1 then 'ltach' else 'snf' end as provider_type
    , source, row_number() over (order by case_id, cin_no, adm_dt, dis_dt, provider, source) as idx
from 
(
    select *
        -- -- Planned enhancement for next iteration. The following block of code is where you would approximate provider type as an aggregate of the services rendered. However, the aggregating function (something akin to a product but in SQL) is different from the top-row aggregation here. So need to do separately.
        -- , TK as SNF_lvl1234 --'SNF: Skilled Nursing Levels 1:4 Claims & Encounters'
        -- , TK as SNF_lvl123only --'SNF: Skilled Nursing Levels 1:3 Only'          
        -- , TK as SNF_lvl4only --'SNF: Skilled Nursing Level 4 Only'          
        -- , TK as SNF_ltc --'SNF: Long-Term Care Associated Claims'          
        -- , TK as SNF_custodialonly --'SNF: Custodial Long-Term Care Only'          
        -- , TK as SNF_suba --'SNF: Sub-Acute Associated Claims'          
        -- , TK as SNF_subaonly --'SNF: Sub-Acute Associated Claims Only'          
        -- , TK as SNF_custodialsubaonly --'SNF: Custodial Sub-Acute Only'  
        -- end as either provider_type (revise how ltach flag is used) or as its own new variables (currently what's coded above), eg a series of snf service flags
        -- -- The following can only also be partitioned by provider_type if (1) provider_type is defined in an inner subquery and (2) provider_type is discrete classifier
        , row_number() over (partition by cin_no, provider, to_date(adm_dt), to_date(dis_dt) order by source asc) as rn
    from 
    (
        --claims universe claims
        select claimid as case_id, carriermemid as cin_no, startdate as adm_dt, enddate as dis_dt, provid as provider
            , case
                    when ltach='yes' then 1
                    else 0  
                end as ltach
            -- Planned enhancement for next iteration. The following doesn't work because claims_universe has no revcode field. See Fri 11/16/2018 9:29 AM email from BShelton. 
            -- , case
            --         when lpad(revcode,4,'0') in ('0191', '0192', '0193', '0194') then 1
            --         else null --not '0' since '0' has a distinct meaning: it nullifies '1' in multiplication at next nested level up.
            --     end as SNF_lvl1234_factor --'SNF: Skilled Nursing Levels 1:4 Claims & Encounters'
            -- , case
            --         when lpad(revcode,4,'0') in ('0191', '0192', '0193') then 1
            --         when lpad(revcode,4,'0') in ('0160', '0194', '0199') then 0
            --         else null
            --     end as SNF_lvl123only_factor --'SNF: Skilled Nursing Levels 1:3 Only'          
            -- , case
            --         when lpad(revcode,4,'0') in ('0194') then 1
            --         when lpad(revcode,4,'0') in ('0160', '0191', '0192', '0193', '0199') then 0
            --         else null
            --     end as SNF_lvl4only_factor --'SNF: Skilled Nursing Level 4 Only'          
            -- , case
            --         when lpad(revcode,4,'0') in ('0160') then 1
            --         else null
            --     end as SNF_ltc_factor --'SNF: Long-Term Care Associated Claims'          
            -- , case
            --         when lpad(revcode,4,'0') in ('0160') then 1
            --         when lpad(revcode,4,'0') in ('0191', '0192', '0193', '0194', '0199') then 0
            --         else null
            --     end as SNF_custodialonly_factor --'SNF: Custodial Long-Term Care Only'          
            -- , case
            --         when lpad(revcode,4,'0') in ('0199') then 1
            --         else null
            --     end as SNF_suba_factor --'SNF: Sub-Acute Associated Claims'          
            -- , case
            --         when lpad(revcode,4,'0') in ('0199') then 1
            --         when lpad(revcode,4,'0') in ('0190', '0191', '0192', '0193', '0160') then 0
            --         else null
            --     end as SNF_subaonly_factor --'SNF: Sub-Acute Associated Claims Only'          
            -- , case
            --         when lpad(revcode,4,'0') in ('199') then 1
            --         when lpad(revcode,4,'0') in ('090', '0191', '0192', '0193', '0194', '0160') then 0
            --         else null
            --     end as SNF_custodialsubaonly_factor --'SNF: Custodial Sub-Acute Only'          
            , 1 as source
        from swat.claims_universe
        where (ltc_claim = 'yes' or snf_claim = 'yes' or suba_claim = 'yes' or ltach = 'yes')

        union
        
        --HOAP CLM and HOAP ENC encounters
        select S2.case_id, S2.cin_no, S2.adm_dt, S2.dis_dt, S2.provider, S2.ltach, S2.source
            -- -- Planned enhancement for next iteration. The following requires solving two issues (1) make analog work in claims universe and (2) aggregate claims to cases while still selecting most recent provider [likely requires 2 separate blocks of code]
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0191', '0192', '0193', '0194') then 1
            --         else null --not '0' since '0' has a distinct meaning: it nullifies '1' in multiplication at next nested level up.
            --     end as SNF_lvl1234_factor --'SNF: Skilled Nursing Levels 1:4 Claims & Encounters'
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0191', '0192', '0193') then 1
            --         when lpad(S2.rev_cd,4,'0') in ('0160', '0194', '0199') then 0
            --         else null
            --     end as SNF_lvl123only_factor --'SNF: Skilled Nursing Levels 1:3 Only'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0194') then 1
            --         when lpad(S2.rev_cd,4,'0') in ('0160', '0191', '0192', '0193', '0199') then 0
            --         else null
            --     end as SNF_lvl4only_factor --'SNF: Skilled Nursing Level 4 Only'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0160') then 1
            --         else null
            --     end as SNF_ltc_factor --'SNF: Long-Term Care Associated Claims'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0160') then 1
            --         when lpad(S2.rev_cd,4,'0') in ('0191', '0192', '0193', '0194', '0199') then 0
            --         else null
            --     end as SNF_custodialonly_factor --'SNF: Custodial Long-Term Care Only'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0199') then 1
            --         else null
            --     end as SNF_suba_factor --'SNF: Sub-Acute Associated Claims'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('0199') then 1
            --         when lpad(S2.rev_cd,4,'0') in ('0190', '0191', '0192', '0193', '0160') then 0
            --         else null
            --     end as SNF_subaonly_factor --'SNF: Sub-Acute Associated Claims Only'          
            -- , case
            --         when lpad(S2.rev_cd,4,'0') in ('199') then 1
            --         when lpad(S2.rev_cd,4,'0') in ('090', '0191', '0192', '0193', '0194', '0160') then 0
            --         else null
            --     end as SNF_custodialsubaonly_factor --'SNF: Custodial Sub-Acute Only'          
        from 
        (
            select S1.case_id, S1.cin_no, S1.adm_dt, S1.dis_dt, S1.provider, S1.rev_cd, S1.type_bill
                , case when S1.provider=LTACH_REF.provid then 1 else 0 end as ltach, S1.source
            from
            (
                --Include non-QNXT records from HOAP (note that TK do i limit records to snfs? did i do this in step 8?
                select C.case_id, H.cin_no, adm_dt, dis_dt
                    -- H0000203 has no nip and the fullname is essentially the same as fullname for H0000621 (Promise)
                    , case when H.provider = 'H0000203' then 'H0000621' else H.provider end as provider
                    , type_bill, rev_cd, 2 as source
                from hoap.clm_case_inpsnf as C
                left join hoap.clm_hdr_inpsnf as H
                on C.case_id = H.case_id
                left join hoap.clm_detail_inpsnf as D
                on H.cl_id = D.cl_id
                where C.bp_code<>'TZGQ'
                union
                select C.case_id, H.cin_no, adm_dt, dis_dt
                    -- H0000203 has no nip and the fullname is essentially the same as fullname for H0000621 (Promise)
                    , case when H.provider = 'H0000203' then 'H0000621' else H.provider end as provider
                    , type_bill, rev_cd, 3 as source
                from hoap.ENC_CASE_INPSNF as C
                left join hoap.enc_hdr_inpsnf as H
                on C.case_id = H.case_id
                left join hoap.enc_detail_inpsnf as D
                on H.cl_id = D.cl_id
                where C.bp_code<>'TZGQ'
            ) as S1
            left join swat.ltach as LTACH_REF on S1.provider=LTACH_REF.provid
        ) as S2
        where (substr(S2.type_bill, 1, 2) in ('21', '22') and lpad(S2.rev_cd, 4, '0') in ('0022', '0160', '0191', '0192', '0193', '0194', '0199'))
        or S2.ltach=1

    ) as S1
) as S2
where rn = 1
;

/*
CORRECT PROVIDER SO IT REFLECTS PROVID (ratherthan fedid, npi, etc), and ADD SNF NAMES
*/

drop table if exists nathalie.hand_corrections;

create table nathalie.hand_corrections
as
select *
from 
(
    select *, provider as provider2 from nathalie.tmp_raw_input  where provider not in ('A0011079', 'H0000553', 'A0004000', 'A0011293', 'A0012854', 'H0000336')
    union
    select *, 'H0000109' as provider2 from nathalie.tmp_raw_input where provider in ('A0011079')
    union
    select *, 'A0004803' as provider2 from nathalie.tmp_raw_input where provider in ('H0000553')
    union
    select *, 'H0000183' as provider2 from nathalie.tmp_raw_input where provider in ('A0004000')
    union
    select *, 'H0000006' as provider2 from nathalie.tmp_raw_input where provider in ('A0011293')
    union
    select *, 'H0002048' as provider2 from nathalie.tmp_raw_input where provider in ('H0000336')
    union
    select *, 'UNK' as provider2 from nathalie.tmp_raw_input where provider in ('A0012854')
    union
    select *, 'UNK' as provider2 from nathalie.tmp_raw_input where provider is null
) as S
;

drop table if exists nathalie.named_providers
;

create table nathalie.named_providers
as
select A.*, PROVNAME_REF.provider_correct, PROVNAME_REF.postdischarge_SNFLTCSAname
from
nathalie.hand_corrections as A
left join
(
    select idx, provider3 as provider_correct, postdischarge_SNFLTCSAname, source, row_number() over(partition by idx order by source asc, isnull(postdischarge_SNFLTCSAname, 'Z')) as rn
    from
    (
    
        --Priority/Source 1: provider field is 10 digit NPI (commonly used for encounters), plandata.provider
        select A.idx, B.provid as provider3, B.fullname as postdischarge_SNFLTCSAname, 1 as source
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.npi
        where substring(B.provid, 1, 1) = 'H' 
        and B.fullname is not null
        
        union

        --Priority/Source 2: provider field is 10 digit NPI (commonly used for encounters), encp.mhc_physician
        select A.idx 
            , B.ph_id as provider3
            , case 
                    when trim(B.last_name) like '%HOSPITAL' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%FOUNDATION' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%TARZANA' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%COUNTY' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%&' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CENTER' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CTR' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name)='2' then trim(B.last_name) 
                    when trim(B.first_name) like 'HOSP%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MED%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CAMPUS%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'REG %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CTR%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CENTER%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MONTE COMM%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'AND %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'OF %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    else concat(trim(B.last_name), trim(B.first_name))
                end as postdischarge_SNFLTCSAname
                , 2 as source
        from nathalie.hand_corrections as A
        left join encp.mhc_physician as B
        on A.provider2 = B.npi
        where concat(trim(B.first_name), trim(B.last_name)) is not null  
        union
    
        --Priority/source 3: provider field matches plandata.provider's provid field.
        select A.idx, B.provid as provider3, B.fullname as postdischarge_SNFLTCSAname, 3 as source
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.provid
        where B.fullname is not null
        union
        
        /*
        correct provider field by looking for matches across reference tables
        */

        --In the absence of a provid match, backup matches to fedid are used. However each fedid may be associated with several names in
        --the reference file. Below provtype is ranked so that a SNF name is attached to the data set where several names may have been 
        --associated to the same prov code in the reference file. 
        --Recall that: For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital.
        
        --Priority/source 4: provider field matches encp.mhc_physician's ph_id field and starts with H --> get provider=ph_id and fullname; not that the first_name='2' etc business is to correct data entry anomalies. 
        select A.idx, B.ph_id as provider3
            , case 
                    when trim(B.last_name) like '%HOSPITAL' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%FOUNDATION' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%TARZANA' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%COUNTY' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%&' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CENTER' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CTR' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name)='2' then trim(B.last_name) 
                    when trim(B.first_name) like 'HOSP%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MED%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CAMPUS%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'REG %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CTR%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CENTER%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MONTE COMM%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'AND %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'OF %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    else concat(trim(B.last_name), trim(B.first_name))
                end  as postdischarge_SNFLTCSAname
            , 4 as source
        from nathalie.hand_corrections as A
        left join encp.mhc_physician as B 
        on A.provider2 = B.ph_id
        where concat(trim(B.first_name), trim(B.last_name)) is not null  
        union
        
        -- Priority/source 5: provider field matches plandata.provider's fedid field and the provid corresponding to that provid starts with H --> get provider=provid (via fedid) and fullname.
        select A.idx, B.provid as provider3, B.fullname as postdischarge_SNFLTCSAname, 5 as source 
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.fedid
        where B.fullname is not null
        
        union

        --Priority/source 6: provider field matches encp.mhc_physician's fed_taxid field and the ph_id corresponding to that fed_taxid starts with H --> get provider=ph_id (via fed_taxid) and fullname 
        select A.idx, B.ph_id as provider3
            , case 
                    when trim(B.last_name) like '%HOSPITAL' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%FOUNDATION' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%TARZANA' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%COUNTY' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%&' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CENTER' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CTR' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name)='2' then trim(B.last_name) 
                    when trim(B.first_name) like 'HOSP%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MED%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CAMPUS%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'REG %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CTR%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CENTER%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MONTE COMM%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'AND %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'OF %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    else concat(trim(B.last_name), trim(B.first_name))
                end as postdischarge_SNFLTCSAname
            , 6 as source 
        from nathalie.hand_corrections as A
        left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
        where concat(trim(B.first_name), trim(B.last_name)) is not null 
        
        union
        
        --Priority/source 7 through 10: Do not apply 'H' requirement; use plandata.provider
        --For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital - Institution For Mental Disease , 16=Community Hospital - Inpatient The fact that there are SNFs indicates that I have a capture problem further upstream

        select A.idx, A.provider2 as provider3, B.fullname as postdischarge_SNFLTCSAname, 7 as source
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.fedid
        where B.provtype in ('88') --this is assigned a higher source value to preserve SNF info as much as possible
        and B.fullname is not null
        union
        select A.idx, A.provider2 as provider3, B.fullname as postdischarge_SNFLTCSAname, 8 as source
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.fedid
        where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
        and B.fullname is not null
        union
        select A.idx, A.provider2 as provider3, B.fullname as postdischarge_SNFLTCSAname, 9 as source
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.fedid
        where B.provtype in ('15', '46')
        and B.fullname is not null
        union
        select A.idx, A.provider2 as provider3, B.fullname as postdischarge_SNFLTCSAname, 10 as source -- there is some kind of match to a fullname
        from nathalie.hand_corrections as A
        left join plandata.provider as B
        on A.provider2 = B.fedid
        where B.fullname is not null
        union
        
        --Priority/source 11 through 12: Do not apply 'H' requirement; use plandata.provider
        
        select A.idx, B.ph_id as provider3
            , case 
                    when trim(B.last_name) like '%HOSPITAL' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%FOUNDATION' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%TARZANA' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%COUNTY' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%&' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CENTER' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CTR' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name)='2' then trim(B.last_name) 
                    when trim(B.first_name) like 'HOSP%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MED%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CAMPUS%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'REG %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CTR%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CENTER%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MONTE COMM%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'AND %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'OF %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    else concat(trim(B.last_name), trim(B.first_name))
                end as postdischarge_SNFLTCSAname
            , 11 as source
        from nathalie.hand_corrections as A
        left join encp.mhc_physician as B 
        on A.provider2 = B.ph_id
        where concat(trim(B.first_name), trim(B.last_name)) is not null  
        
        union            

        select A.idx, B.ph_id as provider3
            , case 
                    when trim(B.last_name) like '%HOSPITAL' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%FOUNDATION' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%TARZANA' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%COUNTY' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%&' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CENTER' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.last_name) like '%CTR' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name)='2' then trim(B.last_name) 
                    when trim(B.first_name) like 'HOSP%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MED%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CAMPUS%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'REG %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CTR%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'CENTER%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'MONTE COMM%' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'AND %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    when trim(B.first_name) like 'OF %' then concat(trim(B.last_name), ' ', trim(B.first_name))
                    else concat(trim(B.last_name), trim(B.first_name))
                end as postdischarge_SNFLTCSAname
            , 12 as source 
        from nathalie.hand_corrections as A
        left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
        where concat(trim(B.first_name), trim(B.last_name)) is not null  
        
        union

        --Priority/source 13: Last resort, use provider2 as fullname and as provider3
        select A.idx, A.provider2 as provider3, A.provider2 as postdischarge_SNFLTCSAname, 13 as source -- there is no match to a fullname, but A.provider may not be null
        from nathalie.hand_corrections as A
        
    ) S
    where postdischarge_SNFLTCSAname is not null
) PROVNAME_REF
on A.idx=PROVNAME_REF.idx
where rn = 1
;


--Group claims into cases

drop table if exists nathalie.tmp_respaned_input
;

create table nathalie.tmp_respaned_input
as
select SD.cin_no, SD.provider, SD.postdischarge_SNFLTCSAname, SD.provider_type, SD.adm_dt, ED.dis_dt
from 
(
    select cin_no, provider, postdischarge_SNFLTCSAname, provider_type, adm_dt, row_number() OVER (PARTITION BY cin_no, provider, provider_type ORDER BY adm_dt asc) as rnsd
    from nathalie.named_providers
    where adm_dt is not null and dis_dt is not null
) as SD
left join
(
    select cin_no, provider, postdischarge_SNFLTCSAname, provider_type, dis_dt, row_number() OVER (PARTITION BY cin_no, provider, provider_type ORDER BY dis_dt asc) as rned
    from nathalie.named_providers
    where adm_dt is not null and dis_dt is not null
) as ED
on SD.cin_no=ED.cin_no and SD.provider=ED.provider and SD.provider_type=ED.provider_type and SD.rnsd=ED.rned 
;

drop table if exists nathalie.tmp_cases
;

create table nathalie.tmp_cases
as
select concat(cin_no, provider, postdischarge_SNFLTCSAname, provider_type, '_', to_date(adm_dt)) as case_id, cin_no, provider, postdischarge_SNFLTCSAname, provider_type, adm_dt, dis_dt
from
(
    select cin_no, provider, postdischarge_SNFLTCSAname, provider_type, adm_dt, concat(cin_no, provider, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) as string)) as rnlink
    from
    (
        select L.cin_no, L.provider, L.postdischarge_SNFLTCSAname, L.provider_type, L.adm_dt as adm_dt, datediff(L.adm_dt, R.dis_dt) as d 
        from 
        (
            select *, concat(cin_no, provider, postdischarge_SNFLTCSAname, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) as string)) as rnstart 
            from nathalie.tmp_respaned_input    
        ) L   
        left join
        (
            select *, concat(cin_no, provider, postdischarge_SNFLTCSAname, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by dis_dt asc) + 1 as string)) as rnstart from nathalie.tmp_respaned_input   
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
            select *, concat(cin_no, provider, postdischarge_SNFLTCSAname, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by dis_dt asc) as string)) as rnend from nathalie.tmp_respaned_input   
        ) L   
        left join
        (
            select *, concat(cin_no, provider, postdischarge_SNFLTCSAname, provider_type, cast(row_number() over (partition by cin_no, provider, provider_type order by adm_dt asc) -1 as string)) as rnend from nathalie.tmp_respaned_input
        ) R
        on L.rnend = R.rnend
    ) X
    where d > 1 or d is null
) E  
on S.rnlink = E.rnlink
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
        , X2.postdischarge_SNFLTCSAname
        , X2.type_postdischargeSNFLTCSA
        , X2.days_until_SNFLTCSA_tmp
        , X2.adm_dt_postdischargeSNFLTCSA
        , X2.dis_dt_postdischargeSNFLTCSA
    from nathalie.prjrea_step8_outpatient as All_inp
    left join
    ( -- select only 1 episode per SNF per case (avoid representing the same SNF multiple times per case)
        select *
        from   
        (
            select cin_no, adm_dt, dis_dt
                , snfltcsa_90dfwd_tmp, days_until_SNFLTCSA_tmp, adm_dt_postdischargeSNFLTCSA, dis_dt_postdischargeSNFLTCSA, id_postdischargeSNFLTCSA, postdischarge_SNFLTCSAname, type_postdischargeSNFLTCSA
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
                        , SNFLTCSA.postdischarge_SNFLTCSAname
                        , SNFLTCSA.provider_type as type_postdischargeSNFLTCSA
                        , case 
                                when datediff(SNFLTCSA.adm_dt, IP.dis_dt) < 0 then 0
                                else datediff(SNFLTCSA.adm_dt, IP.dis_dt)
                            end as days_until_SNFLTCSA_tmp
                    from nathalie.prjrea_step8_outpatient as IP
                    right join --right not left is required in order to limit set to 'has SNF within 90 d'
                    nathalie.tmp_cases as SNFLTCSA
                    on IP.cin_no = SNFLTCSA.cin_no
                    where (days_add(IP.dis_dt, IP.days_until_next_admit) >= SNFLTCSA.adm_dt --keep SNFLTCSA that started before the next IP admit (eliminate SNF that began after next IP admit)
                            or (IP.days_until_next_admit is null and days_add(IP.dis_dt, 90)>SNFLTCSA.adm_dt)) -- allow for adm then snf then no readmit !!!!!THIS IS KEY CORRECTION
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
            from nathalie.tmp_cases as A
            left join plandata.provider as B
            on A.provider = B.provid
            where B.fullname is not null
            union
            select A.*, B.fullname, 2 as source
            from nathalie.tmp_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('88') 
            and B.fullname is not null
            union
            select A.*, B.fullname, 3 as source
            from nathalie.tmp_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('16', '70')
            and B.fullname is not null
            union
            select A.*, B.fullname, 4 as source
            from nathalie.tmp_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            union
            select A.*, B.fullname, 5 as source
            from nathalie.tmp_cases as A
            left join plandata.provider as B
            on A.provider = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            union
            --Below ensures that all SNF cases are kept, whether or not a name was found
            select A.*, A.provider as fullname, 6 as source
            from nathalie.tmp_cases as A
        ) X1
    ) X2
    where rn = 1
    group by fullname, yrmo
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
select A.*, B.admitcount as uniquemember_postdischargeSNFLTCSA_admitsthismonth
from
(
    select *
        , cast(concat(cast(extract(year from adm_dt) as string), lpad(cast(extract(month from adm_dt) as string), 2, '0')) as int) as yrmo
    from nathalie.tmp
) A
left join nathalie.tmp_traffic_monthly B
on A.postdischarge_SNFLTCSAname = B.fullname and A.yrmo = B.yrmo
;


/*
GENERATE ANALYTIC FILE BY REDUCING TO 1 ROW PER HOSPITAL ADMIT

nathalie.prjrea_step8_outpatient has most recent SNF. Contains 1 row per inpatient case. This is the file that is being built for modeling purposes. 

To reduce the file, rather than select the name of the most recent SNF, drop SNF names altogether and compute the existence of a SNF 
(1) at all in 90 d or after index discharge, (2) within 1 day of admission, (3) within 3 days of admission, (4) within 7 days of admission,
(5) within 14 days of admission.
*/

set max_row_size=7mb;

drop table if exists nathalie.prjrea_step9_postdischargeSNF
;

create table nathalie.prjrea_step9_postdischargeSNF
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
from nathalie.prjrea_step8_outpatient as A
left join 
(
    select *
        , 1 as tmpval
    from 
    (
        select cin_no, adm_dt, postdischarge_snfltcsaname, type_postdischargeSNFLTCSA, days_until_snfltcsa, uniquemember_postdischargesnfltcsa_admitsthismonth 
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

DROP TABLE if exists nathalie.tmp_cases; DROP TABLE if exists nathalie.tmp_completedatepairs; DROP TABLE if exists nathalie.tmp_long_cases; DROP TABLE if exists nathalie.tmp_raw_input; 
DROP TABLE if exists nathalie.tmp_respaned_input; DROP TABLE if exists nathalie.tmp_traffic_monthly; DROP TABLE if exists nathalie.tmp_traffic_wholeperiod;
