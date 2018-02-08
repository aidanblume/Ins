-- BINNED 
--[still unsure about proper treatment of near-match duplicates
--note: remember to look at whether you're capturing transfers with this, not just disch-readmits

select --view count of readmissions by amount of time lapsed since index admission
readmission_delay
, count(readmission_delay) as frequency
from
(
  SELECT case
  when days_between is null then 'no readmission in time frame'
  when days_between <0 then 'less than 0'
  when days_between <8 then '00-07'
  when days_between <15 then '08-14'
  when days_between <22 then '15-21'
  when days_between <31 then '22-30'
  else 'more than 30'
  end as readmission_delay
  FROM
  (
          SELECT --create a new table with rows tha capture index admissions in yr=2017 and the next admission for that patient in yr= 2017 or 2018
          I.index_member_id as member_id
          , I.index_admit_date as index_admit_date
          , I.index_discharge_date as index_discharge_date
          , case 
            when I.index_member_id = R.readmit_member_id and I.index_discharge_date <= R.readmit_admit_date then R.readmit_admit_date 
            else null
            end as readmit_admit_date
          , case --
            when I.index_member_id = R.readmit_member_id and I.index_discharge_date <= R.readmit_admit_date then R.readmit_discharge_date 
            else null
            end as readmit_discharge_date
          ,CASE
            when I.index_member_id = R.readmit_member_id and I.index_discharge_date <= R.readmit_admit_date THEN (trunc(R.readmit_ADMIT_DATE) - trunc(I.index_discharge_date)) 
            ELSE NULL
          END AS days_between
          FROM  
          (
            SELECT 
            ROW_NUMBER() OVER(ORDER BY index_member_id ASC, index_admit_date ASC) AS row#
            , index_visit_guid
            , index_member_id
            , index_admit_date
            , index_discharge_date
            from 
            (
              SELECT DISTINCT --there are 61486 records at time this is written
              visit_guid as index_visit_guid
              , member_id as index_member_id
              , TRUNC(admit_date) as index_admit_date
              , TRUNC(discharge_date) as index_discharge_date
              from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
              WHERE ds_visit_type_id = 70 
              AND discharge_date IS NOT NULL
              AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
              AND visit_guid NOT IN
                (
                SELECT visit_guid
                FROM
                  (
                    SELECT visit_guid, count(visit_guid)
                    from 
                        (
                        SELECT DISTINCT
                        visit_guid
                        , member_id as index_member_id
                        , TRUNC(admit_date) as index_admit_date
                        , TRUNC(discharge_date) as index_discharge_date
                        from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
                        WHERE ds_visit_type_id = 70 
                        AND discharge_date IS NOT NULL
                        AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
                        ORDER BY visit_guid 
                        )
                    group by visit_guid
                    having count (visit_guid) > 1
                    )
              )
            ) 
          ) I --for INDEXADMIT
          LEFT JOIN
          (
            SELECT 
            ROW_NUMBER() OVER(ORDER BY index_member_id ASC, index_admit_date ASC) AS row#
            , index_visit_guid as readmit_visit_guid
            , index_member_id as readmit_member_id
            , index_admit_date as readmit_admit_date
            , index_discharge_date as readmit_discharge_date
            from 
            (
              SELECT DISTINCT --there are 61486 records at time this is written
              visit_guid as index_visit_guid
              , member_id as index_member_id
              , TRUNC(admit_date) as index_admit_date
              , TRUNC(discharge_date) as index_discharge_date
              from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
              WHERE ds_visit_type_id = 70 
              AND discharge_date IS NOT NULL
              AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
              AND visit_guid NOT IN
                (
                SELECT visit_guid
                FROM
                  (
                    SELECT visit_guid, count(visit_guid)
                    from 
                        (
                        SELECT DISTINCT
                        visit_guid
                        , member_id as index_member_id
                        , TRUNC(admit_date) as index_admit_date
                        , TRUNC(discharge_date) as index_discharge_date
                        from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
                        WHERE ds_visit_type_id = 70 
                        AND discharge_date IS NOT NULL
                        AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
                        ORDER BY visit_guid 
                        )
                    group by visit_guid
                    having count (visit_guid) > 1
                    )
              )
            ) 
          ) R --for READMIT
          ON I.row# = R.row# - 1
    )
    where extract(year from index_admit_date) in ('2017') -- limit index admission. Readmission can happen in 2018.
)
group by readmission_delay
order by readmission_delay
;
