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

--clean up
drop table #MyTable, #MyTable1, #Mytable2, #MyTable3, #MyTable4, #MyTable5, #MyTable6;


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
-------------


TK
start with #MyTable, has no row#


--------------
*/

SELECT --row_number() over (order by c.memid asc, c.startdate asc, c.enddate asc) as row# 
ALTER TABLE #MyTable
ADD COLUMN row# 
	(
		SELECT row_number() over (order by c.memid asc, c.startdate asc, c.enddate asc) as row#
		FROM #MyTable
	)
;





/*
-------------


TK


--------------
*/


/*
-------------


Clean space


--------------
*/
