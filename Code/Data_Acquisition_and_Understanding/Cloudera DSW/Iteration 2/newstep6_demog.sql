/***
Title:              step4a_demog
Description:        Add demographic data about the member. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step3c_NCQA_LOB
                    encp.members
Output:             nathalie.prjrea_step4a_demog
***/

drop table if exists nathalie.prjrea_step4a_demog;

create table nathalie.prjrea_step4a_demog
as
select 
    *
    , case
            when adm_age is null then null
            when adm_age <= 17 then 'C' --child
            when adm_age > 17 and adm_age < 65 then 'A' --adult
            when adm_age >= 65 and adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_hedis 
    , case
            when adm_age is null then null
            when adm_age < 16 then 'C' --child
            when adm_age >= 16 and adm_age < 26 then 'T' --transition age adult, aka TAY
            when adm_age >= 26 and adm_age < 60 then 'A' --adult
            when adm_age >= 60 and adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_cty 
    , case
            when 1st_of_adm_mth_age is null then null
            when 1st_of_adm_mth_age < 19 then 'C' --child
            when 1st_of_adm_mth_age >= 19 and 1st_of_adm_mth_age < 65 then 'A' --adult
            when 1st_of_adm_mth_age >= 65 and 1st_of_adm_mth_age <= 150 then 'O' --older adult
            else null
        end as agegp_lob_rollup 
from
(
    select
        A.*
        , B2.gender
        , language_written_code
        , ethnicity_code
        , zip_code
        , zip4
        , phone
        , case
                when phone is null then 0
                else 1
            end as has_phone
        , B2.deathdate
        , case 
            when datediff(deathdate, dis_dt) <= 0 then 1
                else 0
            end as dies_before_discharge
        , case
                when deathdate <= adddate(dis_dt, 30) then 1
                else 0
            end as is_a_30d_death
        , B2.dob
        , case
                when dob is null then null
                else floor(datediff(A.adm_dt, B2.dob) / 365.25) 
            end as adm_age 
        , case
                when dob is null then null
                else floor(datediff(trunc(A.adm_dt, 'month'), B2.dob) / 365.25) 
            end as 1st_of_adm_mth_age 
    from nathalie.prjrea_step3c_NCQA_LOB as A 
    left join 
    (
        select *
        from
        (
            select cin_no, gender, language_written_code, ethnicity_code, zip_code, zip4, phone, deathdate, dob
                , row_number() over(partition by cin_no order by dob, gender, zip_code, zip4, language_written_code, ethnicity_code, phone, deathdate) as rownumner1652
            from encp.members 
        ) as B1
        where rownumner1652 = 1 and cin_no is not null
    ) B2
    on A.cin_no = B2.cin_no
    -- where A.cin_no not like '%*ZZ%' and A.cin_no not like '%*01%'
    where not (A.cin_no like '%*ZZ%' or A.cin_no like '%*01%')
    union
    select
        A.*
        , B2.gender
        , language_written_code
        , ethnicity_code
        , zip_code
        , zip4
        , phone
        , case
                when phone is null then 0
                else 1
            end as has_phone
        , B2.deathdate
        , case 
            when datediff(deathdate, dis_dt) <= 0 then 1
                else 0
            end as dies_before_discharge
        , case
                when deathdate <= adddate(dis_dt, 30) then 1
                else 0
            end as is_a_30d_death
        , B2.dob
        , case
                when dob is null then null
                else floor(datediff(A.adm_dt, B2.dob) / 365.25) 
            end as adm_age 
        , case
                when dob is null then null
                else floor(datediff(trunc(A.adm_dt, 'month'), B2.dob) / 365.25) 
            end as 1st_of_adm_mth_age 
    from nathalie.prjrea_step3c_NCQA_LOB as A 
    left join 
    (
        select *
        from
        (
            select mhc_member_no, gender, language_written_code, ethnicity_code, zip_code, zip4, phone, deathdate, dob
                , row_number() over(partition by mhc_member_no order by dob, gender, zip_code, zip4, language_written_code, ethnicity_code, phone, deathdate) as rownumner1652
            from encp.members 
        ) as B1
        where rownumner1652 = 1 and mhc_member_no is not null
    ) B2
    on A.cin_no = B2.mhc_member_no
    where A.cin_no like '%*01%'
        
) as S
--where dis_status not in ('20', '40', '41', '42') --member did not expire before discharge
--and datediff(deathdate, dis_dt) >= 0 --member did not expire before discharge 
;
