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


-- Create a temporary table of enrollid's inpatient services with start and end dates & total paid.
SELECT 
ROW_NUMBER() OVER(ORDER BY enrollid, startdate ASC) AS row#, 
provid, startdate, enddate, facilitycode, totalpaid, enrollid
	INTO #MyTable 
	FROM CSNLACSQL01_Plandata.dbo.claim
	WHERE [status] = 'PAID' and resubclaimid = '' and facilitycode = 1 AND SUBSTRING(CAST(startdate as varchar), 8, 4) = '2017'	   
;
--select top 10 * from #MyTable;

-- Add to each row the number of days since last inpatient service, allowing NULL when the service is the 1st for enrollee in time period of query.
SELECT 
A.row#, A.enrollid, A.startdate, A.enddate, A.provid, A.facilitycode, A.totalpaid, 
B.enddate AS "enddate_of_last_service", 
CASE
	WHEN A.enrollid = B.enrollid THEN DATEDIFF(d, B.startdate, A. enddate)
	ELSE NULL
END AS days_since_last_service
INTO #MyTable2
FROM #MyTable AS A INNER JOIN #MyTable AS B ON B.row# = A.row# - 1
ORDER BY A.row# ASC
;
GO
-- select top 10 * from #MyTable2;
DROP TABLE #MyTable;
GO

--Identify rows where a readmission occurred and save the cost. 
SELECT 
row#, enrollid, startdate, enddate, provid, facilitycode, totalpaid, 
enddate_of_last_service, days_since_last_service,
CASE 
	WHEN days_since_last_service < 2 THEN NULL
	WHEN days_since_last_service <= 30 THEN totalpaid
	WHEN days_since_last_service > 30 THEN NULL
	ELSE NULL
END AS "totalpaid_30d_readmitted"
INTO #MyTable
FROM #MyTable2
ORDER BY row# ASC
;
GO
DROP TABLE #MyTable2;
--select top 10 * from #MyTable;

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