/*Look at 1 member

*/

select visit_guid, admit_date, last_touch_date, discharge_date, ds_visit_status_id, ds_visit_type_id, admit_source_id, admit_type_id
from admit_dischrg_transf_data_snc
where member_id = '90043352D'
and admit_date between '2018-03-03' and '2018-03-17'
--and ds_visit_type_id = '72'
order by admit_date, last_touch_date, ds_visit_type_id, admit_type_id
;


/*ER visits that don't end: intermediate table 

All visit_guid ids for ER visits that never had an inpatient period or a discharge date, 
unless they occurred recently (within last 2 days)
*/

drop table if exists NATHALIE.njb_orphanERvisits_eConnect;

create table NATHALIE.njb_orphanERvisits_eConnect 
as
select A.visit_guid
from
( -- all ER visits that have no discharge dates and are more than 2 days old
    select distinct(visit_guid) as visit_guid
    from admit_dischrg_transf_data_snc
    where
    ds_visit_type_id = '72'
    and discharge_date is null 
    and admit_date < date_sub(now(), 2)
) as A
left anti join -- remove from result above any concluded visit, i.e. belonging to the set of...
(
    -- ... visit_guid that conclude with either inpatient stay or discharge date
    select distinct(visit_guid) as Excluded_visit_guid
    from admit_dischrg_transf_data_snc
    where visit_guid in 
    (
        select distinct(visit_guid)
        from admit_dischrg_transf_data_snc
        where
        ds_visit_type_id = '72'
    )
    and (ds_visit_type_id = '70' or discharge_date is not null)
) as B
on A.visit_guid = B.Excluded_visit_guid
;


/*Folow up: 

can you eliminate those that have an overlapping admit that does end in disch or transfer to inpatient TK
*/
-- not useful. whether or not an ER visit is followed by another admit does not make it more or less anomalous

/*Prevalence of orphan ER visits

*/

--all time points
select count(distinct visit_guid) from nathalie.njb_orphanervisits_econnect; -- 39621
select count(distinct visit_guid) from admit_dischrg_transf_data_snc where ds_visit_type_id = '72' and admit_date < date_sub(now(), 2); --440077
-- 9.00% of ER visits have been orphans

--For past year:
select count(distinct visit_guid) from admit_dischrg_transf_data_snc
where visit_guid in (select visit_guid from nathalie.njb_orphanervisits_econnect)
and admit_date between date_sub(now(), 365) and now()
; -- 25065
select count(distinct visit_guid) from admit_dischrg_transf_data_snc where ds_visit_type_id = '72' 
and admit_date < date_sub(now(), 2)
and admit_date between date_sub(now(), 365) and now()
; --253349
-- 9.89% of ER visits have been orphans



/*For export to dashboard: all visits

checked: the visit_guids are distinct. 
*/

drop table if exists nathalie.njb_tableau_export;

create table nathalie.njb_tableau_export
as
select *
from (
    select *
        , row_number() over (
            partition by visit_guid
            order by last_touch_date desc ) as rnum
    from admit_dischrg_transf_data_snc
    where visit_guid not in ( select visit_guid from nathalie.njb_orphanervisits_econnect )
) S
where rnum = 1
and visit_guid not in (
    select visit_guid
    from nathalie.njb_orphanervisits_econnect
    )
and ds_visit_status_id in ('74', '75') -- 74 is 'in process' and '75' is discharged; there are no other values, so this is superfluous
and admit_date between date_sub(now(), 365) and now() -- 1 years, 365 days
;
--302803



            /*Do stays overlap? 
            
            -- query not completed
            */
            
            select member_id, admit_date, last_touch_date, discharge_date, ds_visit_status_id, ds_visit_type_id, admit_source_id, admit_type_id
            from nathalie.njb_tableau_export
            order by last_name, member_id, admit_date, last_touch_date, ds_visit_type_id, admit_type_id
            ;
            
            select * from nathalie.njb_tableau_export
            where ad;


/* REMAINING ISSUES

--open inventory in tableau community: follow along teh solution
-- ask Tony: 
    -- facility_location is often null. What's teh best way to tell where the data are coing from and where the patient is hospitalized?
    -- discharge_location: see if you can engineer it to be more useful. 
--Display (need to bring in ext data)
    -- of the members, which ones are actively care managed within LA Care?
    -- have a page for Antelope ValleyFuture pr study
    -- by diagnosis
    -- by procedure (for UM?)

*/


            /*Daily census 
            
            */
            create table nathalie.njb_tmp
            as
            select visit_guid, member_id, admit_date, discharge_date from nathalie.njb_tableau_export limit 2;
            
            select * from  nathalie.njb_tmp;
            
            select  t.member_id
                   ,date_add (t.admit_date,pe.i)   as Day
            from    nathalie.njb_tmp t
                    --lateral view 
                    posexplode(split(space(datediff(t.discharge_date,t.admit_date)),' ')) pe as i,x
            
            
            -----
            
            --want to generate a date table
            declare @StartDate datetime
            declare @EndDate datetime
            select @StartDate = '2011-01-01' ,  @EndDate = '2011-08-01'
            
            select @StartDate= @StartDate-(DATEPART(DD,@StartDate)-1)
            
            declare @temp  table
            (
            TheDate datetime
            )
            while (@StartDate<=@EndDate)
            begin
            insert into @temp
            values (@StartDate )
            select @StartDate=DATEADD(MM,1,@StartDate)
            end
            select * from @temp


            --
            create table nathalie.njb_datearray
            as 
            select 
            
            --want to have a table where each patient has a row with a 1 day date
            select *, tmpdate
            from nathalie.njb_tmp
            
            
            
            
            --select result
            
            Select
            
            Date,
            
            PatientId
            
             
            
            from DateSequence as D
            
             
            
            left join
            
            Patients as P
            
            on
            
            P.AdmissionDate<=D.Date
            And
            
            P.DischargeDate>=Date
   

/*
verify that orphans were removed
find out the distribution of inpatient LOS
reproduce the Tableau worksheet using the recording
impose a LOS window for visible inpatient stays
compute census using BS's method
use shell scripting (spark?) to create a date table and implement the census computation that way?
*/

--orphans removed: yes, they were removed. 
-- distribution of LOS

