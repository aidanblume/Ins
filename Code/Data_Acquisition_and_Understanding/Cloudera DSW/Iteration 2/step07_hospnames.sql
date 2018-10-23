/***
Title:              step7_hospnames
Description:        Add the name of the hospital providing service (at this time, this is the 1st provider in a case; ignores info on transfer provider) 
                    TK: Ask client what info they want to see on the transfer facility
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step6_demog 
                    plandata.provider
Output:             nathalie.prjrea_step4b_hospitals is the main data set, not aggregated
                    -- nathalie.prjrea_tblo_index_hospitals: rates are computed over index hospital
                    -- nathalie.prjrea_tblo_readmit_hospitals: rates are computed over readmitting hospital
Issues:             In QNXT's provider table, the provid field matches many but not all cases.
                    A greater number of matches to QNXT.provider's fedid field exist, but there are duplicate fedid values that point simultaneously to
                    hospitals and to individual providers. The provtype field may help specify hospitals, but the values are specific to smaller subsets
                    that need to be unified into a single 'hospital' group. For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 
                    70=Acute Psychiatric Hospital - Institution For Mental Disease , 16=Community Hospital - Inpatient
                    The fact that there are SNFs indicates that I have a capture problem further upstream.
***/ 

-- tmp note: select * from plandata.provider where provid in ('H0000553') - should be 'A0004803', ATLANTIC MEMORIAL HCC, https://atlanticmemorial.com/


--Find all notes about changes in hospital provider id

--***THIS STEP REQUIRES LOOKING AT THE OUTPUT AND ADJUSTING THE QUERY ACCORDINGLY**
--Replace provider ids as instructed
--Cannot use 'update' or 'alter' in Hue. Grr. 

drop table if exists nathalie.hand_corrections;

create table nathalie.hand_corrections
as
select *, provider as provider2 from nathalie.prjrea_step6_demog  where (provider not in ('A0011079', 'H0000553', 'A0004000', 'A0011293', 'A0012854') or provider is null)
union
select *, 'H0000109' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011079')
union
select *, 'A0004803' as provider2 from nathalie.prjrea_step6_demog where provider in ('H0000553')
union
select *, 'H0000183' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0004000')
union
select *, 'H0000006' as provider2 from nathalie.prjrea_step6_demog where provider in ('A0011293')
union
select *, null as provider2 from nathalie.prjrea_step6_demog where provider in ('A0012854')
;

drop table if exists nathalie.prjrea_step7_hospitals
;

create table nathalie.prjrea_step7_hospitals
as
select A.*, PROVNAME_REF.provider_correct, PROVNAME_REF.hospname
from nathalie.hand_corrections as A
left join
(
    select distinct cin_no, adm_dt, dis_dt, provider3 as provider_correct, fullname as hospname
    from
    (
        select cin_no, adm_dt, dis_dt, provider3, fullname, source, row_number() over(partition by cin_no, adm_dt, dis_dt order by source asc, isnull(fullname, 'Z')) as rn
        from
        (
            --Priority/source 1: provider field matches plandata.provider's provid field.
            select A.cin_no, A.adm_dt, A.dis_dt, B.provid as provider3, B.fullname, 1 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where substring(B.provid, 1, 1) = 'H' 
            and B.fullname is not null
            union
            
            /*
            correct provider field by looking for matches across reference tables
            */

            --In the absence of a provid match, backup matches to fedid are used. However each fedid may be associated with several names in
            --the reference file. Below provtype is ranked so that a SNF name is attached to the data set where several names may have been 
            --associated to the same prov code in the reference file. 
            --Recall that: For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital.
            
            --Priority/source 2: provider field matches encp.mhc_physician's ph_id field and starts with H --> get provider=ph_id and fullname; not that the first_name='2' etc business is to correct data entry anomalies. 
            select A.cin_no, A.adm_dt, A.dis_dt, B.ph_id as provider3
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
                , 2 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            union
            
            -- Priority/source 3: provider field matches plandata.provider's fedid field and the provid corresponding to that provid starts with H --> get provider=provid (via fedid) and fullname.
            select A.cin_no, A.adm_dt, A.dis_dt, B.provid as provider3, B.fullname, 3 as source 
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where substring(B.provid, 1, 1) = 'H' 
            union

            --Priority/source 4: provider field matches encp.mhc_physician's fed_taxid field and the ph_id corresponding to that fed_taxid starts with H --> get provider=ph_id (via fed_taxid) and fullname 
            select A.cin_no, A.adm_dt, A.dis_dt, B.ph_id as provider3
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
                , 4 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where substring(B.ph_id, 1, 1) = 'H' 
            and concat(trim(B.first_name), trim(B.last_name)) is not null  
            union
            
            --Priority/source 5 through 8: Do not apply 'H' requirement; use plandata.provider

            select cin_no, A.adm_dt, A.dis_dt, A.provider2 as provider3, B.fullname, 5 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value to preserve SNF info as much as possible
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2 as provider3, B.fullname, 6 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2 as provider3, B.fullname, 7 as source
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2 as provider3, B.fullname, 8 as source -- there is some kind of match to a fullname
            from nathalie.hand_corrections as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            union
            
            --Priority/source 9 through 10: Do not apply 'H' requirement; use plandata.provider
            
            select A.cin_no, A.adm_dt, A.dis_dt, B.ph_id as provider3
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
                , 9 as source
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B 
            on A.provider2 = B.ph_id
            where concat(trim(B.first_name), trim(B.last_name)) is not null  
            union            

            select A.cin_no, A.adm_dt, A.dis_dt, B.ph_id as provider3
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
                , 10 as source 
            from nathalie.hand_corrections as A
            left join encp.mhc_physician as B on A.provider2 = B.fed_taxid
            where concat(trim(B.first_name), trim(B.last_name)) is not null  
            union

            --Priority/source 11: Last resort, use provider2 as fullname and as provider3
            
            select cin_no, A.adm_dt, A.dis_dt, A.provider2 as provider3, A.provider2 as fullname, 11 as source -- there is no match to a fullname, but A.provider may not be null
            from nathalie.hand_corrections as A
            
        ) S
        where fullname is not null
    ) S2
    where rn = 1
) PROVNAME_REF
on A.cin_no = PROVNAME_REF.cin_no
and A.adm_dt = PROVNAME_REF.adm_dt
and A.dis_dt = PROVNAME_REF.dis_dt
;

drop table if exists nathalie.hand_corrections;

