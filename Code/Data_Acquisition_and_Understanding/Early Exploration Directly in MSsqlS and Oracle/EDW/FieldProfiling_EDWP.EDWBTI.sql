/*
EDW data pool claims and encounters. 
Exploration on 20180321.
*/





/*MEM_DEMO_HIST
Will be updated by mid April.
Hoping to get: zip code, lat lon, number address changes in last 12 months, has phone, has secondary phone, has mobile phone, has emergency phone, has emergency contact name, has emergency contact relationship. 
*/

select *
from EDWBTI.MEM_DEMO_HIST
where rownum < 10
;
--query field names. I do not see CIN, only joinkey_mem and mem_bus_key_num. So I need to link joinkey_mem to CIN. WHERE IS THAT TABLE? TK

select count(*)
from EDWBTI.MEM_DEMO_HIST
;
--rowcount = 85,882,176

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
;
--7,872,576 unique members. This is more than the 2M active members we have. So I need to filter out inactive members using another table. WHERE IS THAT TABLE?  TK

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (addr1 is not null or addr2 is not null)
;
--5,173,352 out of 7,872,576 or 66% have a street address. Not all addresses may be current. The current address is X; 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (zip is not null)
;
--5,179,491 zips means we have zips when, in some cases, we don't have street addresses. 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (lat is not null and lngtd is not null)
;
-- 7,202,794 have lat-lon. This is a surprisingly high number. Need to verify address recency as well. Alt: go to "current profile" table WHERE IS THAT TABLE? TK 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (phn is not null or sec_phn is not null or mbl_phn is not null)
;
--7,263,074 out of 7,872,576 (or 92%) have some kind of primary phone information. Does not tell me about recentcy -- some of the numbers may be disconnected. 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (emg_phn is not null)
;
-- 140,551 out of 7,872,576 (or 2%) list an emergency phone number. This is indicative of social support. But: some may use the secondary phone as an emergency contact phone, others may be dependents and therefore there's an assumption that the HOH is the emergency contact person. Affiliation goes beyond this field. 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (sec_phn is not null or emg_phn is not null)
;
--140,879 out of 7,872,576 (or 2%) have either 2nd or emergency phone. 

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (emg_cntct_nm is not null)
;
-- 55 only list an emergency contact name

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (emg_cntct_rltnshp is not null)
;
-- 40





/*MV_MEM
Upcoming fact_members will have better more inclusive info
Hoping to get: DOB, GNDR, MED_INCOM, ETHN, LANG, MARTL
HOH, DEATH_DT, 
What are: IS_VIP, IS_SUB, MULT_BRTH (part of set or parent of set?)
*/

select *
from EDWBTI.MV_MEM
where rownum < 10
;
--query field names. I do not see CIN, only joinkey_mem and mem_bus_key_num. So I need to link joinkey_mem to CIN. 

select count(*)
from EDWBTI.MV_MEM
;
--2,978,366. Need to limit to current members, who should number just north of 2M.

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where DEATH_DT is not null
;
-- 680,163 out of 2,978,366 (4%) which may explain that our active enrollees number: 2,298,203

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (DOB is not null) and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: 2,978,366 out of 2,978,366, or 100% date of birth info present
--WITH LIVING MEMBERS: 2298203 out of 2,298,203, or 100% date of birth info present


select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (GNDR is not null) and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: 2,978,345 out of 2,978,366, or close to 100% gender info present
--WITH LIVING MEMBERS: 2,298,197 out of 2,298,203, or close to 100% date of z info present

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (MED_INCOM is not null) and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: 2,978,366 out of 2,978,366, or 100% median income info present. Surprising --> is it accurate, or are most $0?
--WITH LIVING MEMBERS: 2298203 out of 2,298,203, or 100% date of z info present

select distinct joinkey_mem, med_incom
from EDWBTI.MV_MEM
where rownum <10
; --incomes are reported as $0

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (MED_INCOM is not null)
and (MED_INCOM > 0) and DEATH_DT is null
;
--0 rows with a non-zero income. This field is useless

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (HOH is not null) and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: 2,975,207 out of 2,978,366
--WITH LIVING MEMBERS: 2295046 out of 2,298,203, or close to 100% date of z info present

select HOH
from EDWBTI.MV_MEM
where rownum < 100
;
-- HOH presents as alphanumeric strings. Do these match a CIN?  WHERE IS THE FIELD DEFINITION?

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (ETHN is not null) and (ETHN != 'NO VALID DATA REPORTED') and (ETHN != 'DECLINED TO STATE') and (ETHN != 'OTHER') and (ETHN != 'NOT ON FILE') and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: x out of 2,978,366 or y% of field has some value. 
--WITH LIVING MEMBERS: 256,437 out of 2,298,203, or 11% date of z info present. This is very low.

select distinct(ETHN)
from EDWBTI.MV_MEM
--where rownum < 100
;
-- Good coverage

select distinct(LANG)
from EDWBTI.MV_MEM
;
--good coverage

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where LANG is not null AND lang != 'NOT ON FILE' AND lang != 'OTHER' and DEATH_DT is null
;
--WITH LIVING + DEAD MEMBERS: 2937434 out of 2,978,366 (99%) which is excellent
--WITH LIVING MEMBERS: 2266148 out of 2,298,203, or 99% date of z info present

select LANG, count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where LANG is not null OR lang != 'NOT ON FILE' and DEATH_DT is null
group by LANG
;
--well distributed data

select distinct(MARTL)
from EDWBTI.MV_MEM
;
--good coverage; Note that 'm' and 'M' appear separately --> need to recode into a single value

select count(distinct joinkey_mem)
from EDWBTI.MV_MEM
where (MARTL is not null) and (DEATH_DT is null)
;
--WITH LIVING + DEAD MEMBERS: 113,254 out of 2,978,366 (4%) which is terrible
--WITH LIVING MEMBERS: 91261 out of 2,298,203, or 4% date of z info present. Also terrible.

--What are: IS_VIP, IS_SUB, MULT_BRTH (part of set or parent of set?)

select distinct(IS_VIP)
from EDWBTI.MV_MEM
;
--all 'N'. That's good to know. Weird if there'd been VIPs.

select IS_SUB, count(IS_SUB)
from EDWBTI.MV_MEM
group by IS_SUB
;
--what does "IS_SUB" mean?

select MULT_BRTH, count(MULT_BRTH)
from EDWBTI.MV_MEM
group by MULT_BRTH
;
--only 14 'Y', 2,978,352 'N'






/*FACT_CLM
Hoping to get: 
# of primary care and specialist visits in the past year
number of different types of specialists consulted in the last 12 months (based on services recorded in outpatient records)
# of admissions to hospital by type (emergency versus non-emergency) according to a time interval prior to current admission (90, 180, 365, 730 and 1095 days)
# of hospital admissions during the previous year (0-1; 2-5; >5)(Elixhauser: raw, and also 3+)
# of hospital admissions during the previous 90 days
# of readmissions (last 6 months), 
2 or more readmissions in 30 days in 6 months
# of ER visits in past 6 months (“E” in LACE)(Elixhauser: raw, and also 4+)
# urgent hospital admissions in previous 12 months
# transfers from ER to inpatient in past 6 months (Elixhauser: raw, and also 3+)
Time since last admission (used by Maali et al, 2018)
Acute care utilization: Cumulative length of stay of hospital admissions within the previous year. Cumulative LOS across previous admissions is better proxy for this than number of previous hospital admissions (Maali, 2018)
Site number

NOT MUCH LUCK GETTING THESE THINGS. HAVE SENT EMAIL TO RAHUL.

*/

select * 
from EDWBTI.FACT_CLM
where rownum <100;
--has all three CIN and joinkey_mem and mem_bus_key_num

select count(*)
from EDWBTI.FACT_CLM
;
--20,716,875

/*
# of primary care and specialist visits in the past year
number of different types of specialists consulted in the last 12 months (based on services recorded in outpatient records)
# of admissions to hospital by type (emergency versus non-emergency) according to a time interval prior to current admission (90, 180, 365, 730 and 1095 days)
# of hospital admissions during the previous year (0-1; 2-5; >5)(Elixhauser: raw, and also 3+)
# of hospital admissions during the previous 90 days
*/

select count(*)
from EDWBTI.FACT_CLM
where BILL_TYPE is not null
;
--12,037,870 out of 20,716,875 or 58%

select count(*)
from EDWBTI.FACT_CLM
where MED_REC_NUM is not null
;
--5,841,992 out of 20,716,875 or 58%
--MED_REC_NUM = medical record number

select ADMIT_SRC_CD
from EDWBTI.FACT_CLM
where rownum <100
;
--I don't know what these are. GET DATA DICTIONARY TK. 

select ADMIT_TYPE_CD
from EDWBTI.FACT_CLM
where rownum <100
;
--I don't know what these are. GET DATA DICTIONARY TK. 

select count(distinct PROV_ID)
from EDWBTI.FACT_CLM
;
--101116 unique prov_id

select count(distinct NPI)
from EDWBTI.FACT_CLM
;
--65729 unique NPI (less than provider_id, so maybe provider_id is better populated. 





/*
fact_clm_dtl
HOPE: nothing noted
*/
select * 
from EDWBTI.fact_clm_dtl
where rownum<10
;
--does not look useful


/*
VW_CLM_PROC
Looks like great descriptors per claim, with both code and text, but Rahul had not recognized this as a useful table. Seems useful though?
*/

select * 
from EDWBTI.VW_CLM_PROC
where rownum < 100
;

select distinct(CLM_PROC_CD_DESC)
from EDWBTI.VW_CLM_PROC
;




/*
F_HCOASSIGNMENT
HOLDS A LOT OF PROMISE: need to dig further into this table
Has current address and phone number
Has mileage tp (I guess) PCP, site id, plan code, reasaon for assignment
*/

select * 
from EDWBTI.F_HCOASSIGNMENT
where rownum < 100
;

select safetynet, count(safetynet)
from EDWBTI.F_HCOASSIGNMENT
group by safetynet
;
--Yes	1587084
--No	2051982

select pln_choice, count(pln_choice)
from EDWBTI.F_HCOASSIGNMENT
group by pln_choice
;
--Y	1171938
--N	2467128


 


/*
F_SITE_TRANS_HIST
Hope: 
# changes past site number (indication variation in locations where outpatient care is received)
--> has joinkey_mem, site_no and PCP as well as bgn_dt and end_dt, which allow me to map the member's PCP transitions in 1 table. Awesome!
*/

select * 
from EDWBTI.F_SITE_TRANS_HIST
where rownum < 100
;

--How many unique members are represented?
select count(distinct joinkey_mem) 
from EDWBTI.F_SITE_TRANS_HIST
;
--3,411,442
--need to see which are active








/*
FACT_MTHLY_MEMSHP_QNXT
Hope:
Member eligibility info: plan partner
Member eligibility info: segmentation information
Member eligibility info: provider information
Aid Code
Capitated Aid Code realting to member Eligibility.
*/

select * 
from EDWBTI.FACT_MTHLY_MEMSHP_QNXT
where rownum < 100
;
--join joinkey_mem and joinkey_lob so I can tell what LOBs a member participates in.
-- also pln_prtnr_id, prov_id, ppg_prov_id, gp_cd, cin
--what about aid_cd and captd_aid_cd?

--How many unique members are represented?
select count(distinct joinkey_mem) 
from EDWBTI.FACT_MTHLY_MEMSHP_QNXT
;
-- 2,890,432  !!!!!!!!!I've seen this number before! from here: EDWBTI.MV_MEM: 2,978,366 including the dead. Close. 

select pln_prtnr_id, count(pln_prtnr_id)
from EDWBTI.FACT_MTHLY_MEMSHP_QNXT
group by pln_prtnr_id
order by pln_prtnr_id
;

select aid_cd, count(aid_cd)
from EDWBTI.FACT_MTHLY_MEMSHP_QNXT
group by aid_cd
order by aid_cd
;
--I need data dictionary for aid code. It's just number. TK DICTIONARY

select captd_aid_cd, count(captd_aid_cd)
from EDWBTI.FACT_MTHLY_MEMSHP_QNXT
group by captd_aid_cd
order by captd_aid_cd
;
--I need data dictionary for aid code. It's just number. TK DICTIONARY





/*
DIM_LOB
great reference table to understand the LOB codes
*/

select * 
from EDWBTI.DIM_LOB
where rownum < 100
;





/*
VW_BEN_PLN_CD
great reference table to understand the PLAN codes
*/

select * 
from EDWBTI.VW_BEN_PLN_CD
where rownum < 100
;







/*
MEM_PROV_ASGNMT_HIST
Hope:
Member eligibility info: provider information  ---------->see mm_provider_asgnt_hist table instead
# PCP changes in past x months (indicates difficulty in care management)
Has primary care physician? [indicate extent of relationship. Is it a PCP on paper only, or have they met?)
*/

select * 
from EDWBTI.MEM_PROV_ASGNMT_HIST
where rownum < 100
;

select distinct PCP_TYPE
from EDWBTI.MEM_PROV_ASGNMT_HIST
; -- all PCP

--SPEC_CD could mean specialty code. If so, interesting because I can get provider's specialty, esp as relates to a member's care






/*
MEM_HIST
Hope: 
Family size 
has guardian
# PCP changes in past x months (indicates difficulty in care management)
IDEA: can I capture a severe negative event affecting 1 family member (death, medical emergency, severe chronic disease) and turn that into a risk score for another family member with an illness? Likely to be powerful predictor of negative outcome because of strained social support but also likely to be rare because conjunction of extremes.
*/

select * 
from EDWBTI.MEM_HIST
where rownum < 10
;

select count(*)
from edwbti.mem_hist
;
--38844124

select count(distinct joinkey_mem)
from edwbti.mem_hist
;
--7,756,627

select status, count(status)
from edwbti.mem_hist
group by status
;
/*
null  	0    --- weird because on row inspection, top 10 all are  null
Active	3583252
Inactive	485
ACTIVE	168414
*/

select gndr, count(gndr)
from edwbti.mem_hist
group by gndr
;
/*
	0
Male	17673791
NOT FOUND	2
Female	21170308
*/

select count(distinct joinkey_mem)
from edwbti.mem_hist
where hoh is not null
;
/*
2850129 out of 7,756,627, or 37% coverage
*/

select count(grdn)
from edwbti.mem_hist
where grdn is not null
;
/*
0
*/

select med_incom, count(med_incom)
from edwbti.mem_hist
group by med_incom
;
/*
all null
*/

select ethn, count(ethn)
from edwbti.mem_hist
group by ethn
;
/*
good coverage?
no valid data reported = 237684
declined to state = 12
other = 102571
not on file = 1989
BUT HSPANIC/LATINO = 4!!!!!!!!!  (Mexican = 14699); CHICANO/CHICANA = 1, ECUADORIAN=2, etc. 
*/

select lang, count(lang)
from edwbti.mem_hist
group by lang
;
/*
good coverage; spanish speaking = 1094165, even though few report related ethn
*/

select count(dob)
from edwbti.mem_hist
where dob is null
;
/*
0 missing
*/

select count(death_dt)
from edwbti.mem_hist
where death_dt is not null
;
/*
323404 dead
*/

select martl, count(martl)
from edwbti.mem_hist
group by martl
;
/*
	0
W	1628
M	49561
D	7538
S	55053
m	88
*/

select mult_brth, count(mult_brth)
from edwbti.mem_hist
group by mult_brth
;
/*
	0
Y	17   ----> surprisingly low
N	3752134
*/






/*
FAME_DLY or FAME_MTHLY
many codes/field names in here that I do not understand
something about meds refills is interesting, but is that a benefit code? A prescription code? 
!!!!! It looks like data are sourced differently here, and this is pharmacy stuff. 
Use this to your advantage: you may be able to verify and/or fill in missing data for address, zip, phone number
*/

select *
from edwbti.fame_dly
where rownum < 10
;





/*
MV_ENC_ICD9_DIAG
Reference table for descriptions of ICD9 codes
*/

select *
from edwbti.MV_ENC_ICD9_DIAG
where rownum < 10
;





/*
CLM_STS_HIST
claim status --> may help determine whether a member is on the hook for $$$, which can impact healthcare decisions as well as social stability
Hope:
past claims denied (indicates financial demands are shifted onto member)

To use, I need to join with info about where a denied claim goes next (is it paid under a different benefit? not all goes to the member's wallet) as well as how much the claim was for (so I can compute a total amount owed by the member). 
I wonder whether this analysis has been made before. 
*/

select count(*)
from edwbti.CLM_STS_HIST
;
--77840923






/*
F_MEM_CL
Hope:
Gender
Sex at birth
Is transgender
Sexual orientation


Race/ethnicity
tribe
Primary language / preferred language
Is homeless
Disability
Education levels


has a phone
Cell #1
Cell #1 contact consent
Cell #1 last modified
Cell #1 text consent
cell #2
cell #2 contact consent
Cell #2 last modified
Cell #2 text consent
home phone
home phone conract consent
home phone last update
email
email consent'
email last updated
*/

select count(*)
from edwbti.F_MEM_CL
;
--2,222,207  Good number: is this an accurate count of total number of members?

/*
Gender: GNDR, LAST_MOD_GNDR
Sex at birth:LAST_MOD_SEX_BRTH, SEX_BRTH
Is transgender: "GNDR_IDENT_OTH, GNDR, LAST_MOD_GNDR"
Sexual orientation: LAST_MOD_SEX_ORIENT, SEXUAL_ORIENT_OTH, SEXUAL_ORIENT
*/
select gndr, count(gndr)
from edwbti.F_MEM_CL
group by gndr
;
	0
Other	4
Transgender Female (MTF)	1
Male	126
Female	173
Prefer Not to Answer	22
--small counts
select count(*)
from edwbti.F_MEM_CL
where gndr is null
;
--2221881. This field is terrible at capturing gender, although it does allow for gender fulidity which is informative.
select SEX_BRTH, count(SEX_BRTH)
from edwbti.F_MEM_CL
group by SEX_BRTH
;
--underpopulated
select GNDR_IDENT_OTH, count(GNDR_IDENT_OTH)
from edwbti.F_MEM_CL
group by GNDR_IDENT_OTH
;
--all null
LAST_MOD_SEX_ORIENT, SEXUAL_ORIENT_OTH, SEXUAL_ORIENT
select SEXUAL_ORIENT, count(SEXUAL_ORIENT)
from edwbti.F_MEM_CL
group by SEXUAL_ORIENT
;
--underpopulated



/*
Race/ethnicity
tribe
Primary language / preferred language
Is homeless
Disability
Education levels
*/
select count(*)
from edwbti.F_MEM_CL
;
/*
"ETHN_1 --well populated
ETHN_1_LAC_CD --codes. 
ETHN_2 -- not well populated
ETHN_2_LAC_CD
ETHN_3_LAC_CD
ETHN_3 -- not well populated
ETHN_MAN_UPD --> Y/N , may mean "manual update": indicates that the field is accurate?
ETHN_OTH 
RACE1_CD
RACE1 --well populated
RACE_2_CD
RACE_2
RACE_3_CD
RACE_3
RACE_MAN_UPD_IND
LAST_MOD_ETH
LAST_MOD_RACE
STATE_ETH --> TK need data dictionary; this is a bunch of 1-letters
*/
select STATE_ETH, count(STATE_ETH)
from edwbti.F_MEM_CL
group by STATE_ETH
;
/*
"LAST_MOD_TRIBE, TRIBE_CD -- poorly populated (1 answer)
TRIBE_MAN_UPD
TRIBE_OTH
TRIBE_C -- poorly populated (1 answer)
*/
select TRIBE_C, count(TRIBE_C)
from edwbti.F_MEM_CL
group by TRIBE_C
;
/*
"LAST_MOD_SPOKEN_LANG
LAST_MOD_WRITTEN_LANG
SRC_LANG_EFF_DT
SRC_LANG_CD
SRC_LANG
SPOKEN_LANG_CD
SPOKEN_LANG  -- well populated
SPOKEN_MAN_UPD_IND
SPOKEN_OTH
WRITTEN_CD
WRITTEN_LANG -- well populated
WRITTEN_MAN_UPD_IND
WRITTEN_OTH"
*/
select WRITTEN_LANG, count(WRITTEN_LANG)
from edwbti.F_MEM_CL
group by WRITTEN_LANG
;
/*
Is homeless - HMLESS_IND, LAST_MOD_HMLESS -- poorly populated
Disability -- DISABILITY_STS, LAST_MOD_DIS_STS -- poorly populated
Education levels -- EDU_LVL_OTH, EDU_LVL -- informative but poorly populated
*/
select DISABILITY_STS, count(DISABILITY_STS)
from edwbti.F_MEM_CL
group by DISABILITY_STS
;


/*
has a phone
Cell #1
Cell #1 contact consent
Cell #1 last modified
Cell #1 text consent
cell #2
cell #2 contact consent
Cell #2 last modified
Cell #2 text consent
home phone
home phone conract consent
home phone last update
email
email consent'
email last updated
*/
*/
select count(*)
from edwbti.F_MEM_CL
;
-- no profile bc will aggregate






/*
fact_member_search_mv
HOPE: nothing noted
gndr_id, gender
*/
select GNDR_ID, count(GNDR_ID)
from edwbti.fact_member_search_mv
group by GNDR_ID
;
--good -- but what are "3" and "4"? M or F?
select gender, count(gender)
from edwbti.fact_member_search_mv
group by gender
;
--good -- M/F






/*
mem_enroll_hist
*/

select count(*) 
from edwbti.mem_enrol_hist
;
--empty table






/*
VW_CLM_PROV
*/

select count(*) 
from edwbti.VW_CLM_PROV
;
--empty table

select *
from edwbti.VW_CLM_PROV
where rownum < 10
;
--query repeatedly fails

select *
from edwbti.VW_CLM_PROV
where prov_frst_name = 'John'
;

select prov_qual, count(prov_qual)
from EDWBTI.vw_clm_prov
group by prov_qual
;
/*how do I interpret this> TK NEED DATA DICTIONARY
SY	10
0B	34098
34	85
XX	95606
1G	68065
null	0
24	25
LU	1
EI	59
G2	5101
*/

select prov_spec_cd, count(prov_spec_cd)
from EDWBTI.vw_clm_prov
group by prov_spec_cd
;
--All NA






/*
VW_GRP_CD_SEGMTN
*/
select *
from EDWBTI.VW_GRP_CD_SEGMTN
where rownum < 10
;

select distinct risk_catg_cd
from EDWBTI.VW_GRP_CD_SEGMTN
;
--NEED DICTIONARY TK

select distinct range
from EDWBTI.VW_GRP_CD_SEGMTN
;
--NEED DICTIONARY TK





/*
VW_MEM_PASC
HOPE:
head of household status (clue to whether dependednt or not)
*/
select *
from EDWBTI.VW_MEM_PASC
where rownum < 10
;






/*
VW_PPG
HOPE:
head of household status (clue to whether dependednt or not)
*/

select *
from EDWBTI.VW_PPG
where rownum < 10
;

select distinct(prov_type_desc)
from EDWBTI.VW_PPG
;
--no info

select distinct(prov_ind)
from EDWBTI.VW_PPG
;
--unc or ent --> need data dictionary TK

select distinct(prov_clsfn_cd_desc)
from EDWBTI.VW_PPG
;
--no info

select distinct(prov_sts_type_desc)
from EDWBTI.VW_PPG
;
/*
Invalid
NOT FOUND
None
Valid
*/






/*
VW_PROV_MHC 
*/

select *
from EDWBTI.VW_PROV_MHC
where rownum < 10
;





/*
VW_MEM_HOH
*/

select *
from EDWBTI.VW_MEM_HOH
where rownum < 10
;

select count (distinct joinkey_hoh)
from EDWBTI.VW_MEM_HOH
;
--22,228 is very small


