/***
Title:              step7_hospnames
Description:        Add the name of the hospital providing service (at this time, this is the 1st provider in a case; ignores info on transfer provider) 
                    TK: Ask client what info they want to see on the transfer facility
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step6_demog 
                    plandata.provider
Output:             nathalie.prjrea_step7_hospitals is the main data set, not aggregated
                    -- nathalie.prjrea_tblo_index_hospitals: rates are computed over index hospital
                    -- nathalie.prjrea_tblo_readmit_hospitals: rates are computed over readmitting hospital
Issues:             In QNXT's provider table, the provid field matches many but not all cases.
                    A greater number of matches to QNXT.provider's fedid field exist, but there are duplicate fedid values that point simultaneously to
                    hospitals and to individual providers. The provtype field may help specify hospitals, but the values are specific to smaller subsets
                    that need to be unified into a single 'hospital' group. For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 
                    70=Acute Psychiatric Hospital - Institution For Mental Disease , 16=Community Hospital - Inpatient
                    The fact that there are SNFs indicates that I have a capture problem further upstream.

select provider_correct, hospname from nathalie.prjrea_step7_hospitals where hospname like '%RICHARD%'
select distinct fullname, provid, npi, fedid from plandata.provider where provid='942728480' or npi='942728480' or fedid='942728480'
***/ 

--***THIS STEP REQUIRES LOOKING AT THE OUTPUT AND ADJUSTING THE QUERY ACCORDINGLY**
--Replace provider ids as instructed; often these ids associated with the terms void, duplicate, error. 
--Future Improvement: do this programmatically. Wait until DSI settles on a set of tables for provider identification. 

drop table if exists nathalie.hand_corrections;

create table nathalie.hand_corrections
as
select *
from 
(
    -- select *, provider as provider2 from nathalie.prjrea_step6_demog  where provider not in ('A0011079', 'H0000553', 'A0004000', 'A0011293', 'A0012854', 'H0000084', 'H0000336')
    select *, provider as provider2 from nathalie.prjrea_step6_demog  where provider not in ('A0011079', 'H0000553', 'A0004000', 'A0011293', 'A0012854', 'H0000336')
    union
    select *, 'H0000109' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011079')
    union
    select *, 'A0004803' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000553')
    union
    select *, 'H0000183' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0004000')
    union
    select *, 'H0000006' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011293')
    union
    select *, 'H0002048' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000336')
    union
    select *, 'H0002051' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000051')
    union
    select *, 'H0001705' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0010586')
    union 
    select *, 'H0000176' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000659')
    union
    select *, 'A0008515' as provider2 from nathalie.prjrea_step6_demog where provider like '%H0000535%'
    union
    select *, 'H0000178' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011363')
    union
    select *, 'H0001981' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0001465')
    union
    select *, 'H0000365' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0010326')
    union
    select *, 'H0001716' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000084')
    union
    select *, 'H0000480' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000544')
    union
    select *, 'H0000194' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000580')
    union
    select *, 'H0000459' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0006368')
    union
    select *, 'H0002159' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0004180')
    union
    select *, 'H0001355' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0120036')
    union
    select *, 'H0000309' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0009357')
    union    
    select *, 'H0000170' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011234')
    union
    select *, 'H0002338' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011359')
    union
    select *, 'H0002338' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011359')
    union
    select *, 'H0000208' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011335')
    union
    select *, 'A0011294' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0013227')
    union
    select *, 'QP05638511' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011660')
    union
    select *, 'UNK' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0012854') or provider is null
) as S
;

drop table if exists nathalie.prjrea_step7_hospitals
;

create table nathalie.prjrea_step7_hospitals
as
select A.*, PROVNAME_REF.provider_correct, PROVNAME_REF.hospname
from nathalie.hand_corrections as A
left join
(
    select  case_id, provider3 as provider_correct, fullname as hospname, source
    from
    (
        --create rn with these factors: source asc to prioritize best sources, non-null fullnames (no longer necessary since null fullnames are handled in inner loop), and fullname to guarantee that the same name is captured across cases (avoids name variance).
        select case_id, provider3, fullname, source, row_number() over(partition by case_id order by source asc, isnull(fullname, 'Z'), fullname desc) as rn
        from
        (
        
            --Priority/Source 1: provider field is 10 digit NPI (commonly used for encounters), plandata.provider
            select A.case_id, B.provid as provider3, B.fullname, 10 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.npi
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            and status = 'Active'
            union
            select A.case_id, B.provid as provider3, B.fullname, 11 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.npi
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'
            
            union
        
            --Priority/Source 2: provider field is 10 digit NPI (commonly used for encounters), encp.mhc_physician
            select A.case_id
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
                    end as fullname
                    , 20 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B
            on A.provider2 = B.npi
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())
            and (concat(trim(B.first_name), trim(B.last_name)) like '%MED%' or concat(trim(B.first_name), trim(B.last_name)) like '%HEALTH%' or concat(trim(B.first_name), trim(B.last_name)) like '%HOSP%')
            union
            select A.case_id
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
                    end as fullname
                    , 21 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B
            on A.provider2 = B.npi
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())

            union

        
            --Priority/source 3: provider field matches plandata.provider's provid field.
            select A.case_id, B.provid as provider3, B.fullname, 30 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            and status = 'Active'
            union
            select A.case_id, B.provid as provider3, B.fullname, 31 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'

            union
            
            /*
            correct provider field by looking for matches across reference tables
            */

            --In the absence of a provid match, backup matches to fedid are used. However each fedid may be associated with several names in
            --the reference file. Below provtype is ranked so that a SNF name is attached to the data set where several names may have been 
            --associated to the same prov code in the reference file. 
            --Recall that: For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital.
            
            --Priority/source 4: provider field matches encp.mhc_physician's ph_id field and starts with H --> get provider=ph_id and fullname; not that the first_name='2' etc business is to correct data entry anomalies. 
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 40 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())
            and (concat(trim(B.first_name), trim(B.last_name)) like '%MED%' or concat(trim(B.first_name), trim(B.last_name)) like '%HEALTH%' or concat(trim(B.first_name), trim(B.last_name)) like '%HOSP%')
            union
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 41 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())

            union
            
            -- Priority/source 5: provider field matches plandata.provider's fedid field and the provid corresponding to that provid starts with H --> get provider=provid (via fedid) and fullname.
            select A.case_id, B.provid as provider3, B.fullname, 50 as source 
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            union
            select A.case_id, B.provid as provider3, B.fullname, 51 as source 
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'

            union

            --Priority/source 6: provider field matches encp.mhc_physician's fed_taxid field and the ph_id corresponding to that fed_taxid starts with H --> get provider=ph_id (via fed_taxid) and fullname 
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 60 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())
            and (concat(trim(B.first_name), trim(B.last_name)) like '%MED%' or concat(trim(B.first_name), trim(B.last_name)) like '%HEALTH%' or concat(trim(B.first_name), trim(B.last_name)) like '%HOSP%')
            union
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 61 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%VOID%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%DUPLICATE%' 
            and concat(trim(B.first_name), trim(B.last_name)) not like '%ERROR%'
            and (B.expiration_date is null or B.expiration_date >= now())
            union
            
            --Priority/source 7 through 10: Do not apply 'H' requirement; use plandata.provider
            --For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital - Institution For Mental Disease , 16=Community Hospital - Inpatient The fact that there are SNFs indicates that I have a capture problem further upstream

            select A.case_id, A.provider2 as provider3, B.fullname, 70 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value to preserve SNF info as much as possible
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            and status = 'Active'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 71 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value to preserve SNF info as much as possible
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 80 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            and status = 'Active'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 81 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 90 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and (B.fullname like '%MED%' or B.fullname like '%HEALTH%' or B.fullname like '%HOSP%')
            and status = 'Active'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 91 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            and status = 'Active'
            union
            
            select A.case_id, A.provider2 as provider3, B.fullname, 100 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.npi
            where B.fullname is not null
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 101 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.npi
            where B.fullname is not null
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 110 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where B.fullname is not null
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 111 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where B.fullname is not null
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 120 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.fullname is not null
            and B.fullname is not null and B.fullname not like '%VOID%' and B.fullname not like '%DUPLICATE%' and B.fullname not like '%ERROR%'
            union
            select A.case_id, A.provider2 as provider3, B.fullname, 121 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.fullname is not null
            union
            
            --Priority/source 11 through 12: Do not apply 'H' requirement; use plandata.provider
            
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 130 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where concat(trim(B.first_name), trim(B.last_name)) is not null  
            and (concat(trim(B.first_name), trim(B.last_name)) like '%MED%' or concat(trim(B.first_name), trim(B.last_name)) like '%HEALTH%' or concat(trim(B.first_name), trim(B.last_name)) like '%HOSP%')
            union
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 131 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where concat(trim(B.first_name), trim(B.last_name)) is not null  
            
            union            

            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 141 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where concat(trim(B.first_name), trim(B.last_name)) is not null  
            and (concat(trim(B.first_name), trim(B.last_name)) like '%MED%' or concat(trim(B.first_name), trim(B.last_name)) like '%HEALTH%' or concat(trim(B.first_name), trim(B.last_name)) like '%HOSP%')
            union
            select A.case_id, B.ph_id as provider3
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
                    end as fullname
                , 140 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where concat(trim(B.first_name), trim(B.last_name)) is not null  

            union

            --Priority/source 13: Last resort, use provider2 as fullname and as provider3
            select A.case_id, A.provider2 as provider3, A.provider2 as fullname, 150 as source -- there is no match to a fullname, but A.provider may not be null
            from nathalie.hand_corrections as A
            
        ) S
        where fullname is not null
    ) S2
    where rn = 1
) PROVNAME_REF
on A.case_id=PROVNAME_REF.case_id
-- on A.cin_no = PROVNAME_REF.cin_no
-- and A.adm_dt = PROVNAME_REF.adm_dt
-- and A.dis_dt = PROVNAME_REF.dis_dt
;

drop table if exists nathalie.hand_corrections;
