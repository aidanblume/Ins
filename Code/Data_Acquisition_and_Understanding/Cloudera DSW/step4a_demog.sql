/***
Title:              step4a_demog
Description:        Add demographic data about the member. 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step3_lob_pcp 
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
        , gender
        , language_written_code
        , ethnicity_code
        , zip_code
        , zip4
        , phone
        , case
                when phone is null then 0
                else 1
            end as has_phone
        , deathdate
        , case 
            when datediff(deathdate, dis_dt) <= 0 then 1
                else 0
            end as dies_before_discharge
        , case
                when deathdate <= adddate(dis_dt, 30) then 1
                else 0
            end as is_a_30d_death
        , dob
        , case
                when dob is null then null
                else floor(datediff(adm_dt, dob) / 365.25) 
            end as adm_age 
        , case
                when dob is null then null
                else floor(datediff(trunc(adm_dt, 'month'), dob) / 365.25) 
            end as 1st_of_adm_mth_age 
        from nathalie.prjrea_step3_lob_pcp as A left join encp.members as B on A.cin_no = B.cin_no
) as S
--where dis_status not in ('20', '40', '41', '42') --member did not expire before discharge
--and datediff(deathdate, dis_dt) >= 0 --member did not expire before discharge 
;