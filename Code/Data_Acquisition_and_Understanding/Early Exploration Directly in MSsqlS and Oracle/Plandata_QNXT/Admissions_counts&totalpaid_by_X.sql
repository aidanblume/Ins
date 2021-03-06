/****** Script to SELECT ALL VALID ADMISSIONS  ******/

/*
	1. Count & sum of the costs of all 30 day readmissions in 2017, with contiguous admits reengineered as single admits, and deduped by memberid and admitdate
	2. Break down these stats by:
		a. LOB
		b. Hospital
		c. Unique member
	!!!	d. Subpopulations: 
			i. SPD/Seniors&PersonsWithDisabilities 
			ii. CMC/calMediConnect
		e. Relevance to Risk Adjustment, CM, UM
			i. Risk Adjustment
			ii. CM
			iii. UM
*/


SELECT --row_number() over (order by c.memid asc, c.startdate asc, c.enddate asc) as row# 
--, 
c.claimid
, c.memid
, c.startdate
, c.enddate 
, c.totalpaid
, c.billclasscode 
, case rtrim(ltrim(c.facilitycode)) + '/' + c.billclasscode	
	when '1/1' then 'Inpatient'	when '1/2' then 'Inpatient (Medicare Part B Only)'	when '1/3' then 'Outpatient'
	when '1/4' then 'Other'	when '1/5' then 'Intermediate Care I' when '1/6' then 'Intermediate Care II'
	when '1/7' then 'Intermediate Care III'	when '1/8' then 'Swing Beds'
	when '/' then 'None'Else 'Undefined'end as billclass_desc
, bcc.description as type_of_service
, c.admittype 
, case c.admittype   when '1' then 'Emergency' when '2' then 'Urgent' when '3' then 'Elective' when '4' then 'Newborn'  when '9' then 'Information Not Available'
    when '' then 'N/A' else 'Undefined'  end as admit_type_desc
, case
    when CD.revcodeER = 'Y' and patindex ('%UB%', c.formtype)>0 and C.facilitycode = '1' and (C.billclasscode = '3' or C.billclasscode = '4') then 'Y'
	when CD.revcodeER = 'Y' and c.formtype = '1500' and C.facilitycode = '1' and (C.billclasscode = '3' or C.billclasscode = '4') then 'QA' -- DATA ENTRY ERROR CORRECTION
    when CD.location='23' and c.formtype='1500' then 'Y'
	else 'N'
	end as Emergency2
, P.lob as LOB
, P.provid
, P.providername
, p.npi
, PCP.PCP_Provid as PCP_Provid
, PCP.PCP as PCP
, SEC.SEC_Provid
, SEC.SEC_PCP
, PPG.PPG as Member_PPGH
, COALESCE (CD.Primarydiag, '') as Primarydiag
, COALESCE (CD.ICDVersion, '') as ICDVersion
, M.dob
, M.sex

INTO #MyTable

FROM

/*** VALID CLAIMS 
Valid claims - claim number is always populated - Not a test claim - Not a voided claim - should have DCN - should have a Member ID - update ID should Not be dbo
**/
(select claimid, status, totalamt, totalpaid, totalmemamt, startdate, enddate, cast (paiddate as date) as paiddate, cleandate, provid, reason, mempaidamt, logdate, eligibleamt, totaldeduct
, facilitycode, billclasscode, admittype, memid, enrollid, formtype, frequencycode, okpayby, Dcn, contractid, primaryclaimid, planid, eobeligibleamt, totextpaidamt, createid, orgclaimid
from
[CSNLACSQL01_Plandata].[dbo].[claim] 
where
claimid not in ('','#')
and reason!='test'
and status not in ('VOID')
and claimid not in 
	(select claimid
    from [CSNLACSQL01_Plandata].[dbo].[claim]  
    where status!='VOID' and Dcn is null and memid='' and updateid!='dbo') 
						/*** 
						PAID, ADMIT IN 2017, INPATIENT
						+Selection criteria specific to this modeling iteration
						***/
						and [status] = 'PAID' and resubclaimid = '' and facilitycode = 1 AND SUBSTRING(CAST(startdate as varchar), 8, 4) = '2017'	
	)C 

/*** SERVICE TYPE
To get billclass description **/
left join [CSNLACSQL01_Plandata].[dbo].[billclass] bcc on C.facilitycode=bcc.facilitycode and bcc.billclasscode=C.billclasscode

/*** CLAIM PROVIDER, PAY_TO/VENDOR, AND PROGRAM/LOB INFO
Pay to information is derived from Claim provider ID affiliation and affiliation ID
***/
left join 
(
			select distinct c.claimid
			, p1.provid, p1.fullname as providername, p1.npi, p.provid as payto_provid, p.fullname as payto_provider, p.npi as payto_npi, a.affiliateid, pg.description as lob, p.fedid as payto_fedid
			from [CSNLACSQL01_Plandata].[dbo].[claim] c 
				join [CSNLACSQL01_Plandata].[dbo].[provider] p1 on c.provid = p1.provid
				join [CSNLACSQL01_Plandata].[dbo].[enrollkeys] ek on ek.enrollid=c.enrollid
				join [CSNLACSQL01_Plandata].[dbo].[program] pg on pg.programid=ek.programid
				join [CSNLACSQL01_Plandata].[dbo].[affiliation] a on c.affiliationid = a.affiliationid
				join [CSNLACSQL01_Plandata].[dbo].[provider] p on a.affiliateid = p.provid  
				) P on P.claimid=c.claimid

/*** MEMBER INFO 
Member information is derived from Claim member ID and enrollment ID
**/

left join (
			select distinct
			m.memid
			, m.dob
			, m.sex
			, m.fullname
			, e.lastname
			, e.firstname 
			, ek.enrollid
			, ek.carriermemid
			, ek.programid
			from [CSNLACSQL01_Plandata].[dbo].[claim] c join [CSNLACSQL01_Plandata].[dbo].[member] m on c.memid = m.memid
			join [CSNLACSQL01_Plandata].[dbo].[entity] e on m.entityid = e.entid
			join [CSNLACSQL01_Plandata].[dbo].[enrollkeys] ek on m.memid = ek.memid
			left join [CSNLACSQL01_Plandata].[dbo].[program] pr on pr.programid = ek.programid
			left join [CSNLACSQL01_Plandata].[dbo].[memberpcp] mp on ek.enrollid = mp.enrollid 
			join [CSNLACSQL01_Plandata].[dbo].[provider] p on mp.networkid = p.provid
			) M on C.enrollid = M.enrollid

/** Start Primary PCP - at time of service
Valid PCP information - Claim start date is between member PCP effective and termination date
Members with Primary PCP's are identified from Claim enrollment ID
Some discrepancies were identified where there is more than one Primary PCP for each member that have the same and/or overlapping effective dates. Membership accounting QMEIS - QNxt clean up project.
This issue was acknowledged by flagging PCP as 'DUPE PCP' - Membership accounting (Jessica Fuentes) needs to be alerted of these situations. This flag will serve as QA for such incidents.
***/

left join 
(
			select distinct
			claimid
			, ek.carriermemid
			, p.Provid as PCP_Provid
			, p.fullname as PCP
			from [CSNLACSQL01_Plandata].[dbo].[claim] c 
			left join [CSNLACSQL01_Plandata].[dbo].[enrollkeys] ek  on c.enrollid = ek.enrollid
			left join [CSNLACSQL01_Plandata].[dbo].[memberpcp] mp  on ek.enrollid = mp.enrollid 
				and (c.startdate between mp.effdate and mp.termdate) 
				and mp.pcptype = 'PCP'
				and mp.enrollid != ''
			left join [CSNLACSQL01_Plandata].[dbo].[affiliation] a  on mp.affiliationid = a.affiliationid 
			join [CSNLACSQL01_Plandata].[dbo].[provider] p on a.provid = p.provid
			) PCP on PCP.claimid = C.claimid	
	
/*Start for Secondary PCP - at time of service
Valid Secondary PCP information - Claim start date is between member Secondary PCP effective and termination date - PCP Type = 'SEC'
Members with Secondary PCP's are identified from Claim enrollment ID*/

left join (
			select distinct
			claimid
			, p.Provid as SEC_Provid
			, p.fullname as SEC_PCP
			from [CSNLACSQL01_Plandata].[dbo].[claim] c
			left join [CSNLACSQL01_Plandata].[dbo].[enrollkeys] ek  on c.enrollid = ek.enrollid
			left join [CSNLACSQL01_Plandata].[dbo].[memberpcp] mp  on ek.enrollid = mp.enrollid 
				and (c.startdate between mp.effdate and mp.termdate) 
				and mp.pcptype = 'SEC'
				and mp.enrollid != ''
			left join [CSNLACSQL01_Plandata].[dbo].[affiliation] a  on mp.affiliationid = a.affiliationid 
			join [CSNLACSQL01_Plandata].[dbo].[provider] p on a.provid = p.provid
			) SEC on SEC.claimid = C.claimid

/*** Start Member Primary PPG info - at time of service 
Valid PCP Pay-to (PPG) information - Claim enrollment ID that has valid affiliation with Member PCP information to derive Pay To (PPG) Affiliate ID 
Valid Member PCP information - Claim start date is between member PCP effective and termination date 
Members with PCP's are identified from Claim enrollment ID
***/

left join (
			select distinct
			claimid
			, p.fullname as PPG
				   from [CSNLACSQL01_Plandata].[dbo].[claim] c 
            left join [CSNLACSQL01_Plandata].[dbo].[enrollkeys] ek  on c.enrollid = ek.enrollid
            left join [CSNLACSQL01_Plandata].[dbo].[memberpcp] mp  on ek.enrollid = mp.enrollid 
                and (c.startdate between mp.effdate and mp.termdate)
				and mp.pcptype = 'PCP'
				and mp.enrollid != ''
            left join [CSNLACSQL01_Plandata].[dbo].[affiliation] a  on mp.paytoaffilid = a.affiliationid 
            join [CSNLACSQL01_Plandata].[dbo].[provider] p on a.affiliateid = p.provid 
			) PPG on PPG.claimid = C.claimid

/*******  PRIMARY DIAGNOSIS & ER FLAG 
Primary Diagnosis for HCFA is in sequence '1' and for UB it's labeled as 'Primary'
ER claims are identified in HCFA with POS 23 and for UB it's 045X series rev codes and C.facilitycode = '1' and (C.billclasscode = '3' or C.billclasscode = '4') - see header logic
**************/

left join (select distinct
			C.claimid
			, CD3.revcodeER
			, CD2.location
			, CDX.codeid as Primarydiag
			, CDX.ICDversion
			from 
			[CSNLACSQL01_Plandata].[dbo].[claim] c
			left join
				(select distinct c.claimid, codeid, diagtype, ICDversion from [CSNLACSQL01_Plandata].[dbo].[claim] c 
				join [CSNLACSQL01_Plandata].[dbo].[claimdiag] cdx on c.claimid = cdx.claimid where diagtype in ('1', 'Primary')) CDX on CDX.claimid = c.claimid
			left join 
				  (select distinct c.claimid,
				  location from [CSNLACSQL01_Plandata].[dbo].[claim] c join [CSNLACSQL01_Plandata].[dbo].[claimdetail] cd on c.claimid = cd.claimid where cd.status != 'VOID' and location = '23'
				  ) CD2 on c.claimid = CD2.claimid
			left join 
				  (select distinct c.claimid
				   ,revcodeER='Y'
				  from [CSNLACSQL01_Plandata].[dbo].[claim] c join [CSNLACSQL01_Plandata].[dbo].[claimdetail] cd on c.claimid = cd.claimid 
				  where revcode in ('0450','0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459')
				  ) CD3 on c.claimid = CD3.claimid
		    ) CD on C.claimid = CD.claimid


;


/*
--verify that each row has a unique claimid (yes, that is true --> so no dup claimid)
select top 10 * --claimid, count(claimid)
from #MyTable
--group by claimid
--order by count(*) desc
;
*/
ALTER TABLE #MyTable 
ALTER COLUMN claimid text;




/*
ITERATIVELY LOOK FOR AND GROUP CONTIGUOUS HOSPITAL STAYS

THIS STEP MAY REQUIRE WORKING IN THE WORKBENCH OR IN SAS

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




/*
LOOK AT SEGMENTATION
;
*/

select top 10 * from #MyTable;

select distinct billclasscode from #MyTable;
select top 10 * from [CSNLACSQL01_Plandata].[dbo].billclass;
--SNF, outpatient, hospice - non hosp, [Christian Science Inpatient] are showing up. Use wider exclusion set. 
--may be able to use 'billclass_desc' to filter.
select distinct billclass_desc from #MyTable;
select distinct type_of_service from #MyTable;  [Inpatient (Medicare Part B Only)]
select distinct admittype from #MyTable;
select distinct admit_type_desc from #MyTable; --'elective' shows up. I need to make sure to capture the data needed to use Yonsu/Tony Yiu's logic

select admit_type_desc, Emergency2, count(*) from #MyTable where admit_type_desc in ('Elective', 'Emergency', 'Urgent') group by admit_type_desc, Emergency2 order by admit_type_desc;

select distinct LOB from #MyTable;  --> CMC has 2 values

select 
PCP -- 'HEALTH PLAN, L.A. CARE' --82,669
, count(*)
from #MyTable
group by PCP
order by count(PCP) desc;

select distinct ICDVersion

select count(Primarydiag) from #MyTable where primarydiag is not null; --0 null; all filled (there may be inexact dx codes)

select * from [CSNLACSQL01_Plandata].[dbo].hragroup;



/*

what sub-population of our readmissions you plan to focus on first, 

and provide information on the sub-population’s current readmission trends, costs, and why it was chosen.

*/