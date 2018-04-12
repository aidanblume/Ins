/****** Script to SELECT ALL VALID ADMISSIONS  ******/

/*
	1. Count all 30 day readmissions in a 6 months time period and sum up the cost.
	2. Break down these stats by:
		a. LOB
		b. Hospital
		c. Unique member
		d. Subpopulations: 
			i. SPD/Seniors&PersonsWithDisabilities 
			ii. CMC/calMediConnect
		e. Relevance to Risk Adjustment, CM, UM
			i. Risk Adjustment
			ii. CM
			iii. UM
*/

-- count all admissions & 30 day readmissions in a 6 months period, and sum up the cost for both. Break down by 
-- provid, PPG
--totalpaid
	B.fullname AS "PPG", 
	CSNLACSQL01_Plandata.dbo.provider AS B ON A.provid = B.provid
	GROUP BY B.fullname








--simple version
drop table #MyTable, #MyTable1, #Mytable2, #MyTable3, #MyTable4, #MyTable5, #MyTable6;
--admissions into mytable
--create mytable = all admissions in 2017, with contiguous admits reengineered as single admits, and deduped by memberid and admitdate
SELECT row_number() over (order by memid asc, startdate asc, enddate asc) as row#, claimid, memid, startdate, enddate, billclasscode, admittype, emergency, totalpaid
INTO #MyTable
FROM CSNLACSQL01_Plandata.dbo.claim
WHERE [status] = 'PAID' and resubclaimid = '' and facilitycode = 1 AND SUBSTRING(CAST(startdate as varchar), 8, 4) = '2017'	 
order by row#
;
/*
--verify that each row has a unique claimid (yes, that is true --> so no dup claimid)
select claimid, count(claimid)
from #MyTable
group by claimid
order by count(*) desc
;
*/
--look for contiguous admits by juxtaposing a row with the row that follows it and taking a datedif between discharge and admit. Flag as contiguous stay (1) all with datedif < 2. 
--need to repeat until longest continuous stay is isolated.
SELECT A_row#, A_memid, B_memid, A_claimid, B_claimid, A_startdate, A_enddate, B_startdate, B_enddate, interval, concat(A_claimid, ', ', B_claimid) as claimid_set,
CASE
	WHEN interval < 2 THEN 1
	ELSE 0
END AS contiguous
into #MyTable2
FROM
(
	SELECT 
	A.row# as A_row#, A.memid as A_memid, B.memid as B_memid, A.claimid as A_claimid, B.claimid as B_claimid
	, A.startdate as A_startdate, A.enddate as A_enddate, B.startdate as B_startdate, B.enddate as B_enddate, 
	CASE
		WHEN A.memid = B.memid THEN DATEDIFF(d, A. enddate, B.startdate)
			ELSE NULL
		END AS interval
	FROM #MyTable AS A INNER JOIN #MyTable AS B ON B.row# = A.row# + 1
) AS S
;

--Save stand-alone stays into a more permanent table
select *
into #FinalTable
from #MyTable2
where contiguous = 0
;

--save contiguous stays and assign them a new id (preserve old ids as exclusion set in later step)
drop table #MyTable;
select *
into #MyTable
from #MyTable2
where contiguous = 1
;

drop table #MyTable2;
select * from #MyTable
--join engineered stays and original stays that did not contribute to engineering
select A_memid as memid, claimid_set as claimid, A_startdate as startdate
, case 
	when A_enddate < B_enddate THEN B_enddate
	else A_enddate
	end as enddate
, claimid_set
into #MyTable2
from #MyTable
;

drop table #MyTable
select count(*) from #MyTable2;




--repeat search for contiguous stays
/*
select case
	when sysdatetime() < '2018-04-10 10:21:00.00'
	then 1
	else 0
end as Answer
*/
--WHILE (select count(distinct claimid_set) from #MyTable2) > 0
--BEGIN
	SELECT row_number() over (order by memid asc, startdate asc, enddate asc) as row#, claimid_set, memid, startdate, enddate
	INTO #MyTable3
	FROM #MyTable2	order by row#
	;
	SELECT A_row#, A_memid, B_memid, A_claimid_set, B_claimid_set, A_startdate, A_enddate, B_startdate, B_enddate, interval, concat(A_claimid_set, ', ', B_claimid_set) as claimid_set,
	CASE
		WHEN interval < 2 THEN 1
		ELSE 0
	END AS contiguous
	into #MyTable4
	FROM
	(
		SELECT 
		A.row# as A_row#, A.memid as A_memid, B.memid as B_memid, A.claimid_set as A_claimid_set, B.claimid_set as B_claimid_set
		, A.startdate as A_startdate, A.enddate as A_enddate, B.startdate as B_startdate, B.enddate as B_enddate, 
		CASE
			WHEN A.memid = B.memid THEN DATEDIFF(d, A. enddate, B.startdate)
				ELSE NULL
			END AS interval
		FROM #MyTable3 AS A INNER JOIN #MyTable3 AS B ON B.row# = A.row# + 1
	) AS S
	;
	insert into #FinalTable
	select *
	from #MyTable4
	where contiguous = 0
	;
	select *
	into #MyTable5
	from #MyTable4
	where contiguous = 1
	;
	select A_memid as memid, claimid_set as claimid, A_startdate as startdate
	, case 
		when A_enddate < B_enddate THEN B_enddate
		else A_enddate
		end as enddate
	, claimid_set
	into #MyTable6
	from #MyTable5
	;
	--IF (sysdatetime() > '2018-04-10 10:55:00.00')
	--	BREAK;
	--ELSE
		--CONTINUE;

	--WAITFOR DELAY '00:00:02';
	drop table #MyTable2;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable3;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable4;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable5;
	WAITFOR DELAY '00:00:02';
	select *
	into #MyTable2
	from #MyTable6
	;

	drop table #MyTable6;
--END
select count(distinct claimid_set) from #MyTable2


select * from #MyTable2;

select count(*) from #FinalTable;
select A_memid as memid, A_startdate as startdate, A_enddate as enddate, claimid_set
into #FinalTable1
from #FinalTable;

drop table #FinalTable;
select * from #FinalTable1;
select count(*) from #FinalTable1;
--547371

--look for duplicates memid, admitdate, enddate
select distinct memid, startdate, enddate
into #FinalTable
from #FinalTable1
;
drop table #FinalTable1
select count(*) from #FinalTable;
--547371; all are unique
select count(*) from #FinalTable where enddate is null;
--0
select top 100 * from #FinalTable;


--identify 30d readmissions
drop table #MyTable;
SELECT 
ROW_NUMBER() OVER(ORDER BY memid, startdate ASC) AS row#, 
memid, startdate, enddate
	INTO #AllAdmissions 
	FROM #FinalTable  
;
select top 10 * from #AllAdmissions order by row#;

SELECT 
A.row# as row#, B.enddate AS enddate_of_last_service, A.startdate as readmit_startdate
, CASE
	WHEN A.memid = B.memid THEN DATEDIFF(d, B.enddate, A. startdate)
	ELSE NULL
END AS days_since_last_service
, A.enddate as readmit_enddate, A.memid as A_memid, B.memid as B_memid
INTO #MyTable
FROM #AllAdmissions AS A INNER JOIN #AllAdmissions AS B ON B.row# = A.row# - 1
ORDER BY A.row# ASC
;

select top 10 * from #MyTable; 

select A_memid as memid, readmit_startdate, readmit_enddate, days_since_last_service
into #30dReadmits
from #MyTable
where days_since_last_service <= 30
;

select top 10 * from #30dReadmits; 

select count(*) from #AllAdmissions;
select count(*) from #30dReadmits;
--rate: 142,178/547,371 = .2597




select 
	sum(totalpaid) as total_paid, 
	count(totalpaid) as total_claims
from 
	CSNLACSQL01_Plandata.dbo.claim
	where resubclaimid = '' and [status] = 'PAID'   and facilitycode = 1
	and paiddate >= '2017-07-01' and paiddate <'2018-01-01'






--Mary Q inspired version



/*
This will only include CLAIMS not ENCOUNTERS. 
Pair with similar query on eConnect database to see feasibility on near-streaming data (no claims/encounter differentiation).
Pair with similar query on HOA.HOAP database to see effects of HOA cleaning and of ENC inclusion.
AU: njb
Date: v1 20180409
*/


select 
	sum(a.totalpaid) as total_paid, 
	count(a.totalpaid) as total_claims, 
	sum(a.totalpaid) / count(a.totalpaid) as paid_per_claim,
	a.facilitycode, 
	b.[description] as facility_type
from 
	(select * 
	 from CSNLACSQL01_Plandata.dbo.claim
	 where resubclaimid = '' and [status] = 'PAID'   --TKnjb what is resubclaimid? 
	 ) a
	inner join
	CSNLACSQL01_Plandata.dbo.facility b on a.facilitycode = b.facilitycode
where paiddate > '2017-10-01'
group by a.facilitycode, b.[description]
order by sum(a.totalpaid) desc



/* 
From SQL Server: SQL16TZGPRD2, QNXT replication database: CSNLACSQL01Plandata

REF to Admissions_cost_by_PPG_au=Brandon.sql 

PURPOSE: Supplement total inpatient cost with total inpatient cost for readmissions between 2 and 30 days after discharge, + ratio of 30d_cost to total_cost.

USEFULNESS: I'm still messing around with data. The readmissions cost figures may well be inacurate.
!!!Possible problem: If the cost items are for discrete services rather than discrete inpatient stays, and if a patient could go 2 or more days
without being charged for a service within the same stay, then I erroneously treated 1 inpatient stay as 2 or more inpatient stays. 
*Better idea --> use ADT data in Oracle db sys, or explore inpatient episode codes within current database.

AU: NBlume
PROJECT: Readmissions
*/

/*
SELECT 
B.fullname AS "PPG", A.totalpaid, A.totalpaid_30d_readmitted
FROM
#MyTable AS A
LEFT JOIN 
CSNLACSQL01_Plandata.dbo.provider AS B ON A.provid = B.provid;
*/

SELECT PPG, Total_Paid, Total_Paid_for_30d_Readmissions
, CASE
	WHEN Total_Paid IS NULL THEN NULL
	WHEN Total_Paid != 0 THEN Total_Paid_for_30d_Readmissions/Total_Paid 
	ELSE NULL
END AS "Ratio_30d_to_total_paid"
FROM
(
	SELECT 
	B.fullname AS "PPG", 
	SUM(A.totalpaid) AS "Total_Paid", 
	--SUM(A.totalpaid_30d_readmitted) AS "Total_Paid_for_30d_Readmissions"
	--note: use CASE below because you need to recapture PPG with 0 readmission between 2 and 30 days. 
	CASE
		WHEN SUM(A.totalpaid_30d_readmitted) IS NULL THEN 0
		ELSE SUM(A.totalpaid_30d_readmitted) 
	END AS "Total_Paid_for_30d_Readmissions"
	FROM
	#MyTable AS A
	LEFT JOIN 
	CSNLACSQL01_Plandata.dbo.provider AS B ON A.provid = B.provid
	GROUP BY B.fullname
) AS C
ORDER BY Total_Paid desc, Ratio_30d_to_total_paid
;
GO
DROP TABLE #MyTable;
--DROP TABLE #MyTable2;




