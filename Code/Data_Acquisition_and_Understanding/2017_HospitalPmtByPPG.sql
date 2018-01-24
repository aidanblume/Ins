	
/*From SQL Server: SQL16TZGPRD2, QNXT replication database: CSNLACSQL01Plandata*/

SELECT 
	p.fullname as PPG,
	SUM(c.totalpaid)

FROM
	
	(SELECT provid
	FROM CSNLACSQL01_Plandata.dbo.provider) pr --identifies hospital name
INNER JOIN
	(SELECT provid, startdate, facilitycode, totalpaid, enrollid
	FROM CSNLACSQL01_Plandata.dbo.claim
	WHERE [status] = 'PAID' and resubclaimid = '' and facilitycode = 1 AND SUBSTRING(CAST(startdate as varchar), 8, 4) = '2017') c ON pr.provid = c.provid --identifies hospital claims with a service start date in 2017

-- the following section matches the paid claims to PPG
LEFT JOIN 
	(SELECT enrollid 
	FROM CSNLACSQL01_Plandata.dbo.enrollkeys) ek ON c.enrollid = ek.enrollid
LEFT JOIN 
	(SELECT enrollid, paytoaffilid, effdate, termdate 
	FROM CSNLACSQL01_Plandata.dbo.memberpcp WHERE pcptype = 'PCP' AND enrollid != '') mp ON ek.enrollid = mp.enrollid
LEFT JOIN 
	(SELECT affiliationid, affiliateid 
	FROM CSNLACSQL01_Plandata.dbo.affiliation) aff  ON mp.paytoaffilid = aff.affiliationid 
LEFT JOIN 
	(SELECT fullname, provid
	FROM CSNLACSQL01_Plandata.dbo.provider) p ON aff.affiliateid = p.provid 

WHERE	(c.startdate between mp.effdate and mp.termdate)
GROUP BY p.fullname
ORDER BY SUM(c.totalpaid) desc

