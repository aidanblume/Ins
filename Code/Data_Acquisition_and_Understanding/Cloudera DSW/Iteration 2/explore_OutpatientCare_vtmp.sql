SELECT
    billclass_desc, type_of_service, count(*)
FROM
(

    select 
    C.claimid, C.startdate, C.memid, C.enrollid
    
    , case concat(rtrim(ltrim(c.facilitycode)), '/', c.billclasscode)	
    	when '1/1' then 'Inpatient'	when '1/2' then 'Inpatient (Medicare Part B Only)'	when '1/3' then 'Outpatient'
    	when '1/4' then 'Other'	when '1/5' then 'Intermediate Care I' when '1/6' then 'Intermediate Care II'
    	when '1/7' then 'Intermediate Care III'	when '1/8' then 'Swing Beds' when '2/1' then 'SNF Inpatient'	
    	when '2/2' then 'SNF Inpatient (Medicare Part B Only)' 	when '2/3' then 'SNF Outpatient'
    	when '2/4' then 'SNF Other'	when '2/5' then 'SNF Intermediate Care LI'	when '2/6' then 'SNF Intermediate Care II'
    	when '2/7' then 'SNF Intermediate Care III'	when '2/8' then 'SNF Swing Beds' when '3/1' then 'Home Health Inpatient'	
    	when '3/2' then 'Home Health Inpatient (Medicare Part B Only)' when '3/3' then 'Home Health Outpatient'
    	when '3/4' then 'Home Health Other'	when '3/5' then 'Home Health Intermediate Care I' when '3/6' then 'Home Health Intermediate Care II'	
    	when '3/7' then 'Home Health Intermediate Care III' when '3/8' then 'Home Health Swing Beds' when '4/1' then 'Christian Sci Hosp Inpatient'	
    	when '4/2' then 'Christian Sci Hosp N/A' when '4/3' then 'Christian Sci Hosp Outpatient' when '4/4' then 'Christian Sci Hosp Other'
    	when '4/5' then 'Christian Sci Hosp Intermediate Care I' when '4/6' then 'Christian Sci Hosp Intermediate Care II'
    	when '4/7' then 'Christian Sci Hosp Intermediate Care III'	when '4/8' then 'Christian Sci Hosp Swing Beds'
    	when '5/1' then 'Christian Sci Ext Care Inpatient'	when '5/2' then 'Christian Sci Ext Care N/A'
    	when '5/3' then 'Christian Sci Ext Care Outpatient' when '5/4' then 'Christian Sci Ext Care Other' 	when '5/5' then 'Christian Sci Ext Care Intermediate Care I'
    	when '5/6' then 'Christian Sci Ext Care Intermediate Care II' when '5/7' then 'Christian Sci Ext Care Intermediate Care III' when '5/8' then 'Christian Sci Ext Care Swing Beds'
    	when '6/1' then 'Intermediate Care Inpatient' when '6/2' then 'Intermediate Care N/A' when '6/3' then 'Intermediate Care Outpatient'
    	when '6/4' then 'Intermediate Care Other' when '6/5' then 'Intermediate Care IC I' 	when '6/6' then 'Intermediate Care IC II' when '6/7' then 'Intermediate Care IC III'
    	when '6/8' then 'Intermediate Care IC Swing Beds' when '7/1' then 'Clinic Rural Health'	when '7/2' then 'Clinic HB/I Dial' when '7/3' then 'Clinic Free-Standing'	
    	when '7/4' then 'Clinic Outpatient Rehabilitation Facility (ORF)' when '7/5' then 'Clinic Comprehensive Outpatient Rehabilitation Facility (CORF)'
    	when '7/6' then 'Clinic Community Mental Health Center'	when '7/7' then 'Clinic Federally Qualified Health Center (FQHC)' when '7/8' then 'Clinic Licensed Free Standing Emergency Medical Facility'
    	when '7/9' then 'Clinic Other' 	when '8/1' then 'Special Facility Hospice Non-Hospital'	when '8/2' then 'Special Facility Hospice'
    	when '8/3' then 'Special Facility Ambulatory Surgery Center' when '8/4' then 'Special Facility Free-Standing Birth Center'
    	when '8/5' then 'Special Facility Critical Access Hospital' when '8/6' then 'Special Facility Residential Facility' when '8/9' then 'Special Facility Other' 
    	when '9/9' then 'Reserved For National Use'
    	when '/' then 'None'Else 'Undefined'end as billclass_desc
    
    , bcc.description as type_of_service
    
    
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

) S
group by billclass_desc, type_of_service
