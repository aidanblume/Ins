/***
Title:              step6_demog
Description:        Add demographic data about the member. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step5_PPG_LOB_PCP
                    encp.members
Output:             nathalie.prjrea_step6_demog
***/

drop table if exists nathalie.prjrea_step6_demog;

create table nathalie.prjrea_step6_demog
as
select 
    Ca.*
    , S.gender
    , S.language_written_code
    , S.ethnicity_code
    , S.zip_code
    , S.zip4
    , S.phone
    , S.has_phone
    , S.deathdate
    , S.dies_before_discharge
    , S.is_a_30d_death
    , S.dob
    , S.adm_age 
    , S.First_of_adm_mth_age 
    , case
            when S.adm_age is null then null
            when S.adm_age <= 17 then 'C' --child
            when S.adm_age > 17 and S.adm_age < 65 then 'A' --adult
            when S.adm_age >= 65 and S.adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_hedis 
    , case
            when S.adm_age is null then null
            when S.adm_age < 16 then 'C' --child
            when S.adm_age >= 16 and S.adm_age < 26 then 'T' --transition age adult, aka TAY
            when S.adm_age >= 26 and S.adm_age < 60 then 'A' --adult
            when S.adm_age >= 60 and S.adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_cty 
    , case
            when S.First_of_adm_mth_age is null then null
            when S.First_of_adm_mth_age < 19 then 'C' --child
            when S.First_of_adm_mth_age >= 19 and S.First_of_adm_mth_age < 65 then 'A' --adult
            when S.First_of_adm_mth_age >= 65 and S.First_of_adm_mth_age <= 150 then 'O' --older adult
            else null
        end as agegp_lob_rollup 
from nathalie.prjrea_step5_PPG_LOB_PCP as Ca
left join
(

    select  
        C.*
        , case
                when C.phone is null then 0
                else 1
            end as has_phone
        , case 
            when datediff(C.deathdate, C.dis_dt) <= 0 then 1
                else 0
            end as dies_before_discharge
        , case
                when C.deathdate <= adddate(C.dis_dt, 30) then 1
                else 0
            end as is_a_30d_death
        , case
                when C.dob is null then null
                else floor(datediff(C.adm_dt, C.dob) / 365.25) 
            end as adm_age 
        , case
                when C.dob is null then null
                else floor(datediff(trunc(C.adm_dt, 'month'), C.dob) / 365.25) 
            end as First_of_adm_mth_age 
        , row_number() over(partition by C.case_id order by C.priority asc, C.zip_code, C.zip4, C.phone asc) as rn --ordering in part is meant to avoid nulls
    from 
    (

        --look for cin_no in cin_no field of encp.members
        select
            A.case_id
            , A.adm_dt
            , A.dis_dt
            , B2.*
            , 1 as priority
        from nathalie.prjrea_step5_PPG_LOB_PCP as A 
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
        where not (A.cin_no like '%*ZZ%' or A.cin_no like '%*01%') -- remove non-standard cin_nos. 

        union
        
        --look for cin_no in mhc_member_no field
        select
            A.case_id
            , A.adm_dt
            , A.dis_dt
            , B2.*
            , 2 as priority
        from nathalie.prjrea_step5_PPG_LOB_PCP as A 
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
        
    ) C
    
) as S
on Ca.case_id=S.case_id
where S.rn = 1 or S.rn is null -- there are a few cin_nos with null demog data. Is there another table we can use?
--where dis_status not in ('20', '40', '41', '42') --member did not expire before discharge
--and datediff(deathdate, dis_dt) >= 0 --member did not expire before discharge 
;
