SELECT

C.claimid

FROM 

/*** VALID CLAIMS 
Valid claims - claim number is always populated - Not a test claim - Not a voided claim - should have DCN - should have a Member ID - update ID should Not be dbo
**/
(
    select 
        claimid, status, totalamt, totalpaid, totalmemamt, startdate, enddate, paiddate, cleandate, provid, reason, mempaidamt, logdate, eligibleamt, totaldeduct
        , facilitycode, billclasscode, admittype, memid, enrollid, formtype, frequencycode, okpayby, Dcn, contractid, primaryclaimid, planid
        , eobeligibleamt, totextpaidamt, createid, orgclaimid
    from
    plandata.claim
    where
    claimid not in ('','#')
    and reason!='test'
    and status not in ('VOID')
    and claimid not in 
    	(select claimid
        from claim 
        where status!='VOID' and Dcn is null and memid='' and updateid!='dbo')
) C 

/*** SERVICE TYPE
To get billclass description 
**/
left join billclass bcc on C.facilitycode=bcc.facilitycode and bcc.billclasscode=C.billclasscode

/******* PAY
Valid payment information - pay status not Void - check not Void (void date = '2078-12-31') - mq
Check print date is the date stamp used for Organization determination date. However not all claims have a check number / check print date so the claim paid date (adjudication date) in the Claim table is used.  
Some discrepancies were identified where there is more than one check print date for a claim and they are both Not Voided. 
This issue was acknowledged and corrected by choosing the MAX check print date. - mq
***********/
left join 
(
    Select  distinct pv.claimid, pc.checknbr, pv.amountpaid, cast (pc.checkprintdate as timestamp) as paiddate, py.status
    from claim c
    join payvoucher pv on c.claimid = pv.claimid
    join paycheck pc  on pc.paymentid=pv.paymentid
    join payment py  on py.paymentid=pc.paymentid
    and py.status!='VOID'
    where pc.checknbr in (select checknbr
                        from checkhistory
                        where cast(voiddate as timestamp) = '2078-12-31')
    and pc.checkprintdate=(select max(pc2.checkprintdate) from paycheck pc2 join payvoucher pv2 on pc2.paymentid=pv2.paymentid          
                        and pv2.claimid=c.claimid)
) PY on C.claimid = PY.claimid

/**DETAIL PAY - sum of amounnts in claim detail pulling amounts paid per line and interest paid by line ***/
left join
(
    select 
        distinct claimid, contracteligamt , contractpaidamt, benefitamt, detailamtpaid, detailinterestamt 
    from
    (
        select claimid, sum (conteligamt) as contracteligamt , sum(contractpaid) as contractpaidamt, sum(benefitamt) as benefitamt
            , sum (amountpaid) as detailamtpaid, sum(paydiscount)as detailinterestamt 
        from claimdetail 
        group by claimid 
    ) CP_inner
) CP on CP.claimid = C.claimid

/*** INTEREST 
Interest is derived from two sources 1. Medicare contracts with attached Interest tables 2. Claim line manually calculated and entered as 'INTRST'
Some discrepancies were identified where incorrect contracts are attached with Medicare interest tables that would duplicate calculations for interest.
Also, claim line entered with 'INTRST' are misspelled.
These issues were acknowledged and logic was updated to sum duplicate interests (since the health plan had already paid), and a wild card was used to pull all claim lines with '%IN%'.
2015 06 09 - Interest query update instead of using py.paydiscount from payment table use pv.paydiscount from payvoucher table. These will ID only the true claims that paid interest.
2015 11 17 - Absolute value for pv.paydiscount should be used per Janet G. 
***/
-- dedupe
left join 
(
    Select I2.claimid
        ,count (I2.claimid) as countclaimid
        ,sum (I2.interestpaid) as interestpaid
    from			  
    (
	    Select distinct pv.claimid, abs(pv.paydiscount) as interestpaid
        from claim c
        join payvoucher pv on c.claimid = pv.claimid
        and c.status = 'PAID'
        and pv.paydiscount<0
        join paycheck pc  on pc.paymentid=pv.paymentid
        join payment py  on py.paymentid=pc.paymentid
        and py.status!='VOID'
        where pc.checknbr in (select checknbr
        					from checkhistory
        					where cast(voiddate as timestamp) >= '2078-12-31')
        UNION 
		select c.claimid, cd.amountpaid
        from claim c
        join claimdetail cd on c.claimid=cd.claimid
        where (cast(regexp_extract (cd.servcode, 'IN', 1) as int) > 0 or cast(regexp_extract (cd.revcode, 'IN', 1) as int) > 0) 
        -- where (patindex ('%IN%',cd.servcode)>0 or patindex ('%IN%',cd.revcode)>0) 
        and c.status!='DENIED'
        and cd.amountpaid>0
    ) I2 
    group by I2.claimid
) I1 on I1.claimid = C.claimid

/***CONTRACT INFO***/
left join 
(
    select distinct claimid, ci.contracted 
    --, c.startdate, c.affiliationid, c.contractid, c.enrollid, ci.programid, ci.networkid
    --, ci.effdate as ceff, ci.termdate as cterm, ek.effdate as ekeff, ek.termdate as ekterm    
    from claim c
    left join enrollkeys ek on ek.enrollid=c.enrollid
    and (c.startdate>=ek.effdate and c.startdate<=ek.termdate)
    left join program p on p.programid=ek.programid
    left join contractinfo ci on c.affiliationid=ci.affiliationid
    and c.contractid=ci.contractid
    and ek.programid=ci.programid
    and c.contractnetworkid=ci.networkid
    and (c.startdate>=ci.effdate and c.startdate<=ci.termdate)
    --where c.status in ('PAID','DENIED')
) ci on ci.claimid=c.claimid


/*** CLAIM PROVIDER, PAY_TO/VENDOR, AND PROGRAM/LOB INFO
Pay to information is derived from Claim provider ID affiliation and affiliation ID
***/
left join 
(
	select distinct c.claimid
    	, p1.provid, p1.fullname as providername, p1.npi, p.provid as payto_provid, p.fullname as payto_provider, p.npi as payto_npi, a.affiliateid
    	, pg.description as lob, p.fedid as payto_fedid
	from claim c 
	join provider p1 on c.provid = p1.provid
	join enrollkeys ek on ek.enrollid=c.enrollid
	join program pg on pg.programid=ek.programid
	join affiliation a on c.affiliationid = a.affiliationid
	join provider p on a.affiliateid = p.provid  
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
	from claim c join member m on c.memid = m.memid
	join entity e on m.entityid = e.entid
	join enrollkeys ek on m.memid = ek.memid
	left join program pr on pr.programid = ek.programid
	left join memberpcp mp on ek.enrollid = mp.enrollid 
	join provider p on mp.networkid = p.provid
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
	from claim c 
	left join enrollkeys ek  on c.enrollid = ek.enrollid
	left join memberpcp mp  on ek.enrollid = mp.enrollid 
		and (c.startdate between mp.effdate and mp.termdate) 
		and mp.pcptype = 'PCP'
		and mp.enrollid != ''
	left join affiliation a  on mp.affiliationid = a.affiliationid 
	join provider p on a.provid = p.provid
) PCP on PCP.claimid = C.claimid		

/*Start for Secondary PCP - at time of service
Valid Secondary PCP information - Claim start date is between member Secondary PCP effective and termination date - PCP Type = 'SEC'
Members with Secondary PCP's are identified from Claim enrollment ID*/
left join (
	select distinct
	claimid
	, p.Provid as SEC_Provid
	, p.fullname as SEC_PCP
	from claim c
	left join enrollkeys ek  on c.enrollid = ek.enrollid
	left join memberpcp mp  on ek.enrollid = mp.enrollid 
		and (c.startdate between mp.effdate and mp.termdate) 
		and mp.pcptype = 'SEC'
		and mp.enrollid != ''
	left join affiliation a  on mp.affiliationid = a.affiliationid 
	join provider p on a.provid = p.provid
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
				   from claim c 
            left join enrollkeys ek  on c.enrollid = ek.enrollid
            left join memberpcp mp  on ek.enrollid = mp.enrollid 
                and (c.startdate between mp.effdate and mp.termdate)
				and mp.pcptype = 'PCP'
				and mp.enrollid != ''
            left join affiliation a  on mp.paytoaffilid = a.affiliationid 
            join provider p on a.affiliateid = p.provid 
			) PPG on PPG.claimid = C.claimid
			
/*** Start Member Provider Network info - at time of service 
Valid Network information - Claim enrollment ID that has valid affiliation with Member PCP and PPG information to derive Network ID - Affil Type = 'NETWORK'
Valid Member PCP and PPG information - Claim start date is between member PCP effective and termination date 
Members with PCP's are identified from Claim enrollment ID
***/

left join
(
			select distinct
			claimid
			, e.lastname
			from claim c 
			left join enrollkeys ek  on c.enrollid = ek.enrollid
			left join memberpcp mp  on ek.enrollid = mp.enrollid 
				and (c.startdate between mp.effdate and mp.termdate) 
				and mp.pcptype = 'PCP'
				and mp.enrollid != ''
			join provider p on c.provid = p.provid
			left join affiliation a  on p.provid = a.provid 
				and a.affiltype = 'NETWORK'
			left join provider p2  on mp.networkid = p2.provid
			left join entity e on p2.entityid=e.entid
			) PNET on PNET.claimid = C.claimid 

--  TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
-- /*** Start Pend Codes for clean flag
-- Valid Clean claims - Valid claims that were processed without any PEND codes
-- Pend Codes are put in place by examiners to flag claims that do not have sufficient information to adjudicate the claim correctly. Request for additional information letters are developed. 
-- ***/

-- left join
-- 			(
-- 			 select distinct PND2.claimid, PND2.pendreasonid, PND2.penddate 
--             from claim c
--             left join 
-- 				(select distinct cph.claimid, cph.pendreasonid, cph.penddate,
-- 				count (penddate) as pendcount
-- 				from claim c left join claimpendhistory cph on c.claimid = cph.claimid
-- 				where pendreasonid in ('P07','P09','P10')
-- 				group by cph.claimid, cph.pendreasonid, cph.penddate) PND2 on c.claimid = PND2.claimid
-- 				where pnd2.claimid is not null
-- 			) PND on PND.claimid=C.claimid

/*** Start Adjustment flags
Valid Reversed and Claims Adjustment - Claims that have an Original Claim ID 
Claim ID naming convention having the letter 'A' for Adjustments and 'R' for reversed is not consistent - this format was implemented in October 2014. 
The logic was updated to query all claims that has an Original Claim ID. All reversed claims have the status 'REVERSED' the remaining are Adjustments. Initial Adverse Organization determintations that have been reprocessed will
Not have a Reversed claim ID history.
***/

left join 

				(select distinct
				ADJ2.claimid
				, ADJ2.status
				, ADJ2.orgclaimid
				, ADJ2.Dcn
				, REV.claimid as Rclaimid
				, REV.status as Rstatus
				, REV.orgclaimid as Rorgclaimid
				, REV.Dcn as RDCN
				from claim ADJ2 join (select distinct claimid, status, orgclaimid, dcn from claim where status = 'REVERSED') REV ON REV.orgclaimid = ADJ2.orgclaimid
				where ADJ2.status in ('PAID', 'DENIED')
				) ADJ on C.claimid = ADJ.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
/*******  PDR FLAG 
PDR flag identified by attribute ID's 'MSC000007201' - 1st Appeal 'MSC000007202' - 2nd Appeal 'MSC000007203' - 3rd Appeal with values being Upheld or Overturned
**************/
left join (select  distinct
				c.claimid, q.attributeid, q.description, ca.thevalue
				from claim c
				left join claimattribute ca on ca.claimid=c.claimid
				left join attributegroup ag on ag.attributeid=ca.attributeid
				left join qattribute q on q.attributeid=ca.attributeid
				where q .attributeid in ('MSC000007201'/*, 'MSC000007202', 'MSC000007203'*/)
				) PDR on PDR.claimid = C.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
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
			claim c
			left join
				(select distinct c.claimid, codeid, diagtype, ICDversion from claim c 
				join claimdiag cdx on c.claimid = cdx.claimid where diagtype in ('1', 'Primary')) CDX on CDX.claimid = c.claimid
			left join 
				  (select distinct c.claimid, location from claim c join claimdetail cd on c.claimid = cd.claimid where cd.status != 'VOID' and location = '23'
				  ) CD2 on c.claimid = CD2.claimid
			left join 
				  (select distinct c.claimid
				   ,revcodeER='Y'
				  from claim c join claimdetail cd on c.claimid = cd.claimid 
				  where revcode in ('0450','0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459')
				  ) CD3 on c.claimid = CD3.claimid
		    ) CD on C.claimid = CD.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
/*******  PLACE OF SERVICE / LOCATION
Place of service only takes location for claim line '1' as claims can have multiple places of service
**************/
left join 
				  (select distinct c.claimid,
				  location from claim c join claimdetail cd on c.claimid = cd.claimid where cd.status != 'VOID' and cd.claimline = 1
				  ) CT on C.claimid = CT.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
/** Start LTC Flag 
Bill Type 21X,22X,23X Location / POS 13,31 ,32,33,34 -- LTC Claims POD logic Dec 2016
***/
left join (
				select distinct c.claimid, '1' as LTC 
				from claim c join claimdetail cd on cd.claimid=c.claimid
				where (c.facilitycode = '2' 
				and c.billclasscode in ('1','2','3','4','5','6','7','8'))
				or cd.location in ('13','31','32','33')
				or cd.revcode in ('0119', '0889')
				or cd.revcode between '0160' and '0169'
				or cd.revcode between '0184' and '0185'
				or cd.revcode between '0190' and '0199'
				) LTC on LTC.claimid = C.claimid

/** Start CBAS Flag 
CBAS claims use revenue code '3103' and can have HCPCS 'S5102','H2000','T1023'
***/
left join (
				select distinct c.claimid, 1 as CBAS 
				from claim c join claimdetail cd on cd.claimid=c.claimid
				where (cd.servcode in ('S5102','H2000','T1023')
				or cd.revcode='3103')
				) CBAS on CBAS.claimid = C.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
/** Start LTSS Flag 
LTSS claims use revenue code '160','184','185','889','169','119','3103','191','192','193','194','195','196','197','198','199' 
and can have HCPCS/CPT 'S5102', 'T1023', 'H2000', '99339','99340' and location '31', '32', '33' and facility code = 2
Location '34' excluded per new Nursing Facility criteria per S. Noguera - mq 2016 07 22
Rev code '0161-0168' included per new Nursing Facility criteria per S. Noguera - mq 2016 07 22
***/
left join (
				select distinct c.claimid, 1 as LTSS--, c.status, pg.description as program, c.dcn
				from claim c join claimdetail cd on cd.claimid=c.claimid
				where (cd.servcode in ('S5102','H2000','T1023', '99339','99340')
				or cd.revcode in ('0160','0161','0162','0163','0164','0165','0166','0167','0168','0169'
				,'0184','0185','0889','0119','3103','0190', '0191','0192','0193','0194','0195','0196','0197','0198','0199'))
				or location in ('13', '31', '32', '33')
				or c.facilitycode = '2' 
				) LTSS on LTSS.claimid = C.claimid

/*Paper vs EDI
2015 09 16 maryq
--CreateID is a value that the system generates examiners are not able to manipulate this ID. 
--Create ID identified by Jon Armstrong and Janet Ghattas
	'205367462' for 'Paper'
	'330897513','592715634','SERVICES\lacprbatch' for 'EDI'
	Examiner initials are manually entered claims accounted for in 'Paper' claims
--Original claim IDs' Create ID are carried over to its Secondary Claim IDs'
*/
left join (
				  select claimid, c.createid
				  from claim c 
				  join (select orgclaimid, createid from claim where orgclaimid != '') oc on oc.orgclaimid = c.claimid
				  union 
				  select claimid, createid
				  from claim 
				  ) PE on PE.claimid = C.claimid

 TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
/*** DUPLICATE - claims denied as Duplicate Claims ***/ 
left join
			(select distinct c.claimid
			 from claim c join (
			 select 
					distinct cdt.claimid 
					from claimedit cdt 
					where (cdt.ruleid = '915' and cdt.reason in ('D24','D71','M0030','M0051','M3'))
					or (cdt.ruleid in ('307','519','522','531','532','533','534','535','998')) 
				    or cdt.reason in ('D24','D71','M0030','M0051','M3')
					and cdt.status = 'DENY'
					and cdt.ruleid != ''
			   ) D1 on D1.claimid = c.claimid
			)D on D.claimid = C.claimid 

--  TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
-- /* Incomplete/Invalid/No Enrollment/Not Eligible Denials - 2017 05 08 mq OMT flag*/
-- left join
-- 			(select distinct c.claimid
-- 			 from claim c join (
-- 					select 
-- 					distinct cdt.claimid
-- 					from claimedit cdt 
-- 					where 
-- 					(cdt.ruleid = '915' and cdt.reason in ('021','022', '220','D02','D03','D04','D06','D07','D15', 'D15A', 'D16','D102','D103','D105','D106',
-- 					'D12','D14','D26','D27','D28','D44','D52','D53','D54','D55','D56','D57','D58','D62','D63','D69','D73','D78','D79','D80','D81','D84','D85','D92',
-- 					'D94','D98','D99', 'D101', 'M0010','M0011','M0013','M0015', 'M0017','M0018','M0019','M0025','M0026','M0027','M0028','M0031','M0032','M0033',
-- 					'M0034','M0035','M0039','M0040','M0042','M0043','M0045','M0048', 'M0050','M0053','M0054','M0057','M0060','M0072','M0073','M0074','P06','R07','R313','R321',
-- 					'R322','R323','R324','R325','A002','D109','D110','D28','D43','D44','D56','D70','D77F','D83','M0023','M0038','M0041','M0062','M0064','P01','P02', 'P03',
-- 					'R173','R301', 'R302', 'R306','R309','R323') )	
-- 					or 
-- 					(cdt.ruleid in ('101','102','105','106', '107', '140','150','158','162','163','168','172','175','176','179', '204','205','210','214','217','218','224','225','230','245'
-- 					,'250','252','253','258','271','283','301','303','304','305','306','308','309','328','329','330','334','335','336','337'
-- 					,'338','346','353','354','367','384','388','409','504','505','507','508','511','512','515','518','521','523','525','530','538','551','603','606'
-- 					,'609','610','611', '612','635','902','911','913','916','919','920','921','922','989','990','993','995','1111',
-- 					'149','169','173','174','185','293','313','316','359','360','362','377','378','503','550','643','659','966', '111')	)
-- 					or
-- 					(cdt.reason in ('021','022', '220','D02','D03','D04','D06','D07','D15', 'D15A', 'D16','D102','D103','D105','D106',
-- 					'D12','D14','D26','D27','D28','D44','D52','D53','D54','D55','D56','D57','D58','D62','D63','D69','D73','D78','D79','D80','D81','D84','D85','D92',
-- 					'D94','D98','D99', 'D101', 'M0010','M0011','M0013','M0015', 'M0017','M0018','M0019','M0025','M0026','M0027','M0028','M0031','M0032','M0033',
-- 					'M0034','M0035','M0039','M0040','M0042','M0043','M0045','M0048', 'M0050','M0053','M0054','M0057','M0060','M0072','M0073','M0074','P06','R07','R313','R321',
-- 					'R322','R323','R324','R325','A002','D109','D110','D28','D43','D44','D56','D70','D77F','D83','M0023','M0038','M0041','M0062','M0064','P01','P02', 'P03',
-- 					'R173','R301', 'R302', 'R306','R309','R323') )
-- 					and cdt.status = 'DENY'
-- 					and cdt.ruleid != '' 
-- 					) ic on c.claimid=ic.claimid
-- 		    ) IV on IV.claimid = C.claimid

--  TK THIS MAY BE IMPORTANT BUT THE TABLE IS NOTIN HIVE
-- /*** FORWARDING INFO - STANDARD REPORTING V2 - This logic overstates Forwards as it flags Forward&Paid and Forward&Denied claims as well
-- -- Included Forward Remit for Forward claims edit 915 without a reason code - mq 2017 04 24
-- **/ 

-- left join
			
-- 			(select distinct c.claimid
-- 			 from claim c join (
-- 					select 
-- 					distinct cdt.claimid
-- 					from claimedit cdt 
-- 					where 
-- 					(cdt.ruleid = '915' and cdt.reason in ('A001','Beacon','CC','CC1','CC2','CMC','CMC1','CMC2','CMC3','CVPG','D108','D90','DHS','DHS2','HK1','LAC002','R310','D0001'
-- 												, '024','CAP01','D104','D34','D64','Forward to PBM','M0014','M0022','M0056','M0070','P001','PBM','R310','R326','CMC001','CMC002'
-- 												, 'CAP02', 'D104'))
-- 					or (cdt.ruleid in ('169','409','376') and cdt.reason in ('A001','Beacon','CC','CC1','CC2','CMC','CMC1','CMC2','CMC3','CVPG','D108','D90','DHS','DHS2','HK1','LAC002','R310','D0001'
-- 												, '024','CAP01','D104','D34','D64','Forward to PBM','M0014','M0022','M0056','M0070','P001','PBM','R310','R326','CMC001','CMC002'
-- 												, 'CAP02', 'D104'))
-- 				    or (cdt.ruleid in ('151','602','603', '404','408', '153', '1136','178','231','961'))				
-- 					and cdt.status = 'DENY'
-- 					and cdt.ruleid != '' 
-- 					UNION
-- 					select distinct claimid from claimremit
-- 					where msgnumber in ('A002', 'A0100', 'A0101', 'A0624', 'A0625','A0626',          
-- 					'AAREV5', 'D12', 'D13', 'D14', 'D23', 'D45', 'D55', 'D72', 'D73', 'D74', 'D75', 'D76', 'D77','D77A', 'D77B','D77C','D77D','D77E','D77F', 'D78',
-- 					'D79', 'D80', 'D81',  'D88','D89', 'M0019', 'P123', 'R101') 
-- 				   ) f1 on f1.claimid = c.claimid
-- 		    )F on F.claimid = C.claimid 

/*was commented out in original --where c.status = 'DENIED' and c.paiddate between '2016-11-01' and getdate ()*/
-- group by C.claimid,  C.primaryclaimid, C.status, C.totalamt, PY.amountpaid, C.totalmemamt, CBAS , C.totalpaid, C.eobeligibleamt, C.totextpaidamt, C.planid, C.cleandate
-- , PY.claimid, C.paiddate, PY.paiddate, I1.claimid, I1.interestpaid, C.contractid, PND.claimid, F.claimid, CD.revcodeER, C.formtype, C.facilitycode, ci.contracted, c.orgclaimid
-- , C.billclasscode, CD.location, P.lob, P.npi, ADJ.claimid, PDR.claimid, PDR.thevalue, P.provid, P.providername, P.payto_provid, P.payto_provider, C.startdate, C.enddate, LTSS
-- , M.enrollid, M.fullname, M.carriermemid, PCP.PCP_Provid, PCP.PCP, SEC.SEC_Provid, SEC.SEC_PCP, PPG.PPG, PNET.lastname, C.frequencycode, C.admittype, C.reason, C.Dcn
-- , C.okpayby, bcc.description, CD.ICDversion, CD.Primarydiag, C.createid, C.mempaidamt, CT.location, PE.createid, M.lastname, M.firstname, M.dob , M.memid, M.sex, P.payto_npi, PY.checknbr
-- , P.payto_fedid, C.logdate, D.claimid, LTC.LTC, CP.contractpaidamt, CP.benefitamt, CP.detailamtpaid, CP.detailinterestamt, CP.contracteligamt, C.eligibleamt, C.totaldeduct, IV.claimid

group by C.claimid,  C.primaryclaimid, C.status, C.totalamt, PY.amountpaid, C.totalmemamt, CBAS , C.totalpaid, C.eobeligibleamt, C.totextpaidamt, C.planid, C.cleandate
, PY.claimid, C.paiddate, PY.paiddate, I1.claimid, I1.interestpaid, C.contractid
-- , PND.claimid
-- , F.claimid
, CD.revcodeER, C.formtype, C.facilitycode, ci.contracted, c.orgclaimid


, C.billclasscode, CD.location, P.lob, P.npi, ADJ.claimid, PDR.claimid, PDR.thevalue, P.provid, P.providername, P.payto_provid, P.payto_provider, C.startdate, C.enddate, LTSS
, M.enrollid, M.fullname, M.carriermemid, PCP.PCP_Provid, PCP.PCP, SEC.SEC_Provid, SEC.SEC_PCP, PPG.PPG, PNET.lastname, C.frequencycode, C.admittype, C.reason, C.Dcn
, C.okpayby, bcc.description, CD.ICDversion, CD.Primarydiag, C.createid, C.mempaidamt, CT.location, PE.createid, M.lastname, M.firstname, M.dob , M.memid, M.sex, P.payto_npi, PY.checknbr
, P.payto_fedid, C.logdate, D.claimid, LTC.LTC, CP.contractpaidamt, CP.benefitamt, CP.detailamtpaid, CP.detailinterestamt, CP.contracteligamt, C.eligibleamt, C.totaldeduct, IV.claimid
