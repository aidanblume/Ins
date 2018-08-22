/***
Title:              LOS_by_facility_type
Description:        Select all claims for either Acute Inpatient, SNF pr LTC from Jan 2017 onward, and compute length of stay (LOS)
Version Control:    https://dsghe.lacare.org/bshelton/dme_controls/new/LOS_by_facility_type/supporting_analysis/los_distributions
Data Source:        plandata 
Output:             nathalie.prjdme_LOS_by_facility_type
***/

/*
Select start and end dates for all valid inpatient claims, segregated by facility type (LTC, SNF & inpatient acute). 
*/

DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type
;

CREATE TABLE nathalie.tmp_claims_by_facility_type
AS
SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
FROM 
(

    SELECT memid, 'LTC' as facilitytype, startdate, enddate 
    FROM
    (
        SELECT memid, startdate, enddate, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
        FROM
        (
            SELECT C.memid, C.startdate, C.enddate, row_number() over(partition by C.memid, C.startdate order by C.enddate desc) as rnum1
            FROM 
            --Chee's valid claims
           (
                select 
                    memid, claimid, startdate, enddate
                from
                plandata.claim
                --Valid claims, as defined in Mary Q.'s claims universe
                where
                resubclaimid = '' and status = 'PAID'and totalpaid > 0
                --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
                and startdate >= '2017-01-01'
            ) C 
            -- --Mary Q.'s valid claims
            -- (
            --     select 
            --         memid, claimid, startdate, enddate
            --     from
            --     plandata.claim
            --     --Valid claims, as defined in Mary Q.'s claims universe
            --     where
            --     claimid not in ('','#')
            --     and reason!='test'
            --     and status not in ('VOID')
            --     and claimid not in 
            --     	(select claimid
            --         from plandata.claim 
            --         where status!='VOID' and Dcn is null and memid='' and updateid!='dbo')
            --     --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
            --     and startdate >= '2017-01-01'
            -- ) C 
            where C.claimid in
            (
                select count(c1.claimid)
                from plandata.claim c1
                join plandata.claimdetail cd 
                on cd.claimid=c1.claimid
                /*** LTC
                from Mary Q.'s claims universe: appears to include all SNFs without a distinction between SNF and LTC. Do not use. 
                **/
                -- where (c.facilitycode = '2' and c.billclasscode in ('1','2','3','4','5','6','7','8'))
                -- or cd.`location` in ('13','31','32','33')
                -- or cd.revcode in ('0119', '0889')
                -- or cd.revcode between '0160' and '0169'
                -- or cd.revcode between '0184' and '0185'
                -- or cd.revcode between '0190' and '0199'
                /*** LTC
                from Mason (actuary) via Chee's powerpoint 'Pricing_Hospital_Admission_From_Claims_Data' powerpoinut document
                **/
                where lpad(cd.revcode,4,'0') in ('0022', '0160', '0199')
                or lpad(cd.revcode,4,'0') between '0184' and '0185'
                or lpad(cd.revcode,4,'0') between '0190' and '0194'
                
                
            ) 
        ) S1
        where rnum1 = 1
    )S2
    where rnum2 = 1

    union
    
    SELECT memid, 'SNF' as facilitytype, startdate, enddate
    FROM 
    (
        SELECT memid, startdate, enddate, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
        FROM
        (
            SELECT C.memid, C.startdate, C.enddate, row_number() over(partition by C.memid, C.startdate order by C.enddate desc) as rnum1
            FROM 
            --Chee's valid claims
           (
                select 
                    memid, claimid, startdate, enddate
                from
                plandata.claim
                --Valid claims, as defined in Mary Q.'s claims universe
                where
                resubclaimid = '' and status = 'PAID'and totalpaid > 0
                --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
                and startdate >= '2017-01-01'
            ) C 
            -- --Mary Q.'s valid claims
            -- (
            --     select 
            --         memid, claimid, startdate, enddate
            --     from
            --     plandata.claim
            --     --Valid claims, as defined in Mary Q.'s claims universe
            --     where
            --     claimid not in ('','#')
            --     and reason!='test'
            --     and status not in ('VOID')
            --     and claimid not in 
            --     	(select claimid
            --         from plandata.claim 
            --         where status!='VOID' and Dcn is null and memid='' and updateid!='dbo')
            --     --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
            --     and startdate >= '2017-01-01'
            -- ) C 
            where C.claimid in
            (
                select c1.claimid
                from plandata.claim c1
                join plandata.claimdetail cd 
                on cd.claimid=c1.claimid
                /*** LTC
                from Mary Q.'s claims universe, using Mason's logic to exclude LTCs 
                **/
                where (c1.facilitycode = '2' and c1.billclasscode in ('1','2', '5', '6', '7', '8')) -- 'SNF Inpatient','SNF Inpatient (Medicare Part B Only)', 'SNF Intermediate Care LI', 'SNF Intermediate Care II', 'SNF Intermediate Care III', 'SNF Swing Beds'
                and lpad(cd.revcode, 4, '0') not in ('0022', '0160', '0199', '0184', '0185', '0190', '0191', '0192', '0193', '0194') --not LTC
            )
        ) S1
        where rnum1 = 1
    )S2
    where rnum2 = 1
    
    union
    
    SELECT memid, 'Inpatient Acute' as facilitytype, startdate, enddate
    FROM 
    (
        SELECT memid, startdate, enddate, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
        FROM
        (
            SELECT C.memid, C.startdate, C.enddate, row_number() over(partition by C.memid, C.startdate order by C.enddate desc) as rnum1
            FROM     
            --Chee's valid claims
           (
                select 
                    memid, claimid, startdate, enddate
                from
                plandata.claim
                --Valid claims, as defined in Mary Q.'s claims universe
                where
                resubclaimid = '' and status = 'PAID'and totalpaid > 0
                --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
                and startdate >= '2017-01-01'
            ) C 
            -- --Mary Q.'s valid claims
            -- (
            --     select 
            --         memid, claimid, startdate, enddate
            --     from
            --     plandata.claim
            --     --Valid claims, as defined in Mary Q.'s claims universe
            --     where
            --     claimid not in ('','#')
            --     and reason!='test'
            --     and status not in ('VOID')
            --     and claimid not in 
            --     	(select claimid
            --         from plandata.claim 
            --         where status!='VOID' and Dcn is null and memid='' and updateid!='dbo')
            --     --time frame per Brandon Shelton's specs (email from BS to Nathalie Blume dated Thu 7/12/2018 11:47 AM)
            --     and startdate >= '2017-01-01'
            -- ) C 
            where C.claimid in
            (
                select c1.claimid
                from plandata.claim c1
                join plandata.claimdetail cd 
                on cd.claimid=c1.claimid
                where (c1.facilitycode = '1' and c1.billclasscode in ('1','2', '5', '6', '7', '8')) --'Inpatient', 'Inpatient (Medicare Part B Only)', 'Intermediate Care I', 'Intermediate Care II', 'Intermediate Care III', 'Swing Beds' 
            )
        )S1
        where rnum1 = 1  
    )S2
    where rnum2 = 1

)S3
;



/* 
Contiguous stays:

Group together into a single continuous stay claims for the same member at the same facility type that are 1 day or less apart.
This concerns immediate readmissions as well as claims reported across the end of the month.
Example: MEM00001313313 requires 3 passes through the code block below.



MANUALLY RUN THIS SET OF CODE BLOCKS AS LONG AS THE TABLE SIZE DECREASES


*/

DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_2
;

CREATE TABLE nathalie.tmp_claims_by_facility_type_2
AS
SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
FROM 
(
    SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
    FROM (
        SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
        FROM 
        (
            select A.memid, A.facilitytype, A.startdate
                , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
            from nathalie.tmp_claims_by_facility_type as A
            left join nathalie.tmp_claims_by_facility_type as B
            on B.rnum3 = A.rnum3 + 1
        ) S1
    ) S2
    where rnum1 = 1  
) S3
where rnum2 = 1
;

DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type
;

CREATE TABLE nathalie.tmp_claims_by_facility_type
AS
SELECT * FROM nathalie.tmp_claims_by_facility_type_2
;

/*
Compute LOS
*/

drop table if exists nathalie.prjdme_LOS_by_facility_type
;

create table nathalie.prjdme_LOS_by_facility_type
as
select memid, facilitytype, startdate, enddate, datediff(enddate, startdate) as LOS
from nathalie.tmp_claims_by_facility_type
;



/*
Alternative to block above: The following juxtaposes the block 10 times in order to reduce time manually looping through 
*/

-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_2
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_3
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_4
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_5
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_6
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_7
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_8
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_9
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_10
-- ;
-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_11
-- ;


-- --------------------------

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_2
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type as A
--             left join nathalie.tmp_claims_by_facility_type as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;


-- CREATE TABLE nathalie.tmp_claims_by_facility_type_3
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_2 as A
--             left join nathalie.tmp_claims_by_facility_type_2 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_4
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_3 as A
--             left join nathalie.tmp_claims_by_facility_type_3 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_5
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_4 as A
--             left join nathalie.tmp_claims_by_facility_type_4 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_6
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_5 as A
--             left join nathalie.tmp_claims_by_facility_type_5 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_7
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_6 as A
--             left join nathalie.tmp_claims_by_facility_type_6 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_8
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_7 as A
--             left join nathalie.tmp_claims_by_facility_type_7 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_9
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_8 as A
--             left join nathalie.tmp_claims_by_facility_type_8 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_10
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_9 as A
--             left join nathalie.tmp_claims_by_facility_type_9 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type_11
-- AS
-- SELECT memid, facilitytype, startdate, enddate, row_number() over(order by memid, startdate asc, enddate desc) as rnum3
-- FROM 
-- (
--     SELECT *, row_number() over(partition by memid, enddate order by startdate asc) as rnum2
--     FROM (
--         SELECT *, row_number() over(partition by memid, startdate order by enddate desc) as rnum1
--         FROM 
--         (
--             select A.memid, A.facilitytype, A.startdate
--                 , case when (A.memid = B.memid and A.facilitytype = B.facilitytype and datediff(B.startdate, A.enddate) <= 1) then B.enddate else A.enddate end as enddate
--             from nathalie.tmp_claims_by_facility_type_10 as A
--             left join nathalie.tmp_claims_by_facility_type_10 as B
--             on B.rnum3 = A.rnum3 + 1
--         ) S1
--     ) S2
--     where rnum1 = 1  
-- ) S3
-- where rnum2 = 1
-- ;



-- --------------------------




-- DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type
-- ;

-- CREATE TABLE nathalie.tmp_claims_by_facility_type
-- AS
-- SELECT * FROM nathalie.tmp_claims_by_facility_type_11
-- ;







/*
Compute LOS
*/

drop table if exists nathalie.prjdme_LOS_by_facility_type
;

create table nathalie.prjdme_LOS_by_facility_type
as
select memid, facilitytype, startdate, enddate, datediff(enddate, startdate) as LOS
from nathalie.tmp_claims_by_facility_type
;



/*
Verify that LOS(acute)<LOS(SNF)<LOS(LTC)
*/

select facilitytype, count(*), round(avg(LOS), 2)
from nathalie.prjdme_LOS_by_facility_type
group by facilitytype
;

/*
Cleanup
*/

DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type
;

DROP TABLE IF EXISTS nathalie.tmp_claims_by_facility_type_2
;


select count(*) from nathalie.prjdme_LOS_by_facility_type where (memid = '' or memid is null)
