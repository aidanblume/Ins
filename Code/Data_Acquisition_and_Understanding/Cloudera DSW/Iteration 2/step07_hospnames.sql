/***
Title:              step7_hospnames
Description:        Add the name of the hospital providing service (at this time, this is the 1st provider in a case; ignores info on transfer provider) 
                    TK: Ask client what info they want to see on the transfer facility
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step6_demog 
                    plandata.provider
Output:             nathalie.prjrea_step7_hospitals is the main data set, not aggregated
Issues:             In QNXT's provider table, the provid field matches many but not all cases.
                    A greater number of matches to QNXT.provider's fedid field exist, but there are duplicate fedid values that point simultaneously to
                    hospitals and to individual providers. The provtype field may help specify hospitals, but the values are specific to smaller subsets
                    that need to be unified into a single 'hospital' group. For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 
                    70=Acute Psychiatric Hospital - Institution For Mental Disease , 16=Community Hospital - Inpatient
                    The fact that there are SNFs indicates that I have a capture problem further upstream.
***/ 

--Find all notes about changes in hospital provider id


--***THIS STEP REQUIRES LOOKING AT THE OUTPUT AND ADJUSTING THE QUERY ACCORDINGLY**
--Replace provider ids as instructed
--Cannot use 'update' or 'alter' in Hue. Grr. 

drop table if exists nathalie.tmp;

create table nathalie.tmp
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
select A.*, PROVNAME_REF.hospname
from nathalie.tmp as A
left join
(
    select distinct cin_no, adm_dt, dis_dt, fullname as hospname
    from
    (
        select cin_no, adm_dt, dis_dt, provider2, fullname, source, row_number() over(partition by cin_no, adm_dt, dis_dt order by source asc, provider2 desc, fullname asc) as rn
        from
        (
            --A provid match is prefered and is taken to be unique.
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, B.fullname, 1 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.provid
            where B.fullname is not null
            union
            --In the absence of a provid match, backup matches to fedid are used. However each fedid may be associated with several names in
            --the reference file. Below provtype is ranked so that a SNF name is attached to the data set where several names may have been 
            --associated to the same prov code in the reference file. 
            --Recall that: For provtype, 88=snf, 15=Community Hospital - Outpatient, 46=Rehab Clinic, 70=Acute Psychiatric Hospital.
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, B.fullname, 2 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('88') --this is assigned a higher source value because you want to preserve SNF info as much as possible
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, B.fullname, 3 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('16', '70')  --this is assigned the next highest source value so that potential inpatient hosp. that are not rehab are preserved
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, B.fullname, 4 as source
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype in ('15', '46')
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, B.fullname, 5 as source -- there is some kind of match to a fullname
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is not null
            union
            select cin_no, A.adm_dt, A.dis_dt, A.provider2, A.provider2 as fullname, 6 as source -- there is no match to a fullname, but A.provider may not be null
            from nathalie.tmp as A
            left join plandata.provider as B
            on A.provider2 = B.fedid
            where B.provtype not in ('88', '70', '16', '15', '46')
            and B.fullname is null
        ) S
        where fullname is not null --unnecessary since a name is guaranteed in the inner query
    ) S2
    where rn = 1
) PROVNAME_REF
on A.cin_no = PROVNAME_REF.cin_no
and A.adm_dt = PROVNAME_REF.adm_dt
and A.dis_dt = PROVNAME_REF.dis_dt
;

drop table if exists nathalie.tmp;

