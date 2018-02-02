/*
DESCRIPTION: Exploration of the eConnect/ADT database 
AU: Nathalie Blume
PROJECT: Readmission Rate Reduction

ACTIONS:
Contact Tony Truong to get answers to the following questions:
-
-
-
*/

--ROW SELECTION. NOTE DIFFERENCE BETWEEN INDEX AND RELATIVE(30d) READMISSIONS
SELECT *
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
AND ds_visit_type_id = 70
AND DS_VISIT_STATUS_ID = 75 --visits resulting in discharge (not in progress or loss of contact)
--AND DS_VISIT_STATUS_ID IN (75, 74) --discharged or inprocess, use for the readmit for full capture
;

--HOW WELL POPULATED IS language?
--numerator
SELECT COUNT(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70
AND DS_VISIT_STATUS_ID = 75 
AND primary_lang_id IS NOT NULL
;
--denominator
SELECT COUNT(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70
AND DS_VISIT_STATUS_ID = 75 
;
--ANS:~60%

--WHAT IS THE COUNT BY PATIENT CLASS? 
--Only interesting in that it confirms that I have filtered in all inpatient visit types. 
--Could use this as filter and see if I retain non-inpatient visits.
SELECT patient_class_id
, CASE
    WHEN patient_class_id=60 THEN 'all other patients status'
    WHEN patient_class_id=59 THEN 'emergency room'
    WHEN patient_class_id=62 THEN 'inpatient'
    WHEN patient_class_id=136 THEN 'lab'
    WHEN patient_class_id=99 THEN 'observation'
    WHEN patient_class_id=86 THEN 'outpatient'
    WHEN patient_class_id=61 THEN 'pre'
    WHEN patient_class_id=98 THEN 'psychiatry'
    WHEN patient_class_id=156 THEN 'no xref'
    WHEN patient_class_id=155 THEN 'unset'
    ELSE NULL
    END AS patient_class_description
, COUNT(patient_class_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY patient_class_id
;

--WHAT IS THE COUNT BY PATIENT CLASS? 
--Interestingly there is a great deal of variety in the counts here. 
--You may want to use this to reinforce your inpatient filter, OR...
--You may want to use this as a candidate predictor as long as you get business understanding of the codes (e.g. Med/Surg vs. inpatient, which is... medical not surgical?) 
--But be careful. The fact that many members are labeled "outpatient" below despite your "inpatient visit" filter, is a red flag as to the correctness of this data field.
SELECT patient_type_id
, CASE
    WHEN patient_type_id=108 THEN 'community programs'
    WHEN patient_type_id=25 THEN 'emergency room'
    WHEN patient_type_id=106 THEN 'home health service'
    WHEN patient_type_id=28 THEN 'inpatient'
    WHEN patient_type_id=135 THEN 'lab'
    WHEN patient_type_id=104 THEN 'med/surg'
    WHEN patient_type_id=101 THEN 'newborn'
    WHEN patient_type_id=139 THEN 'newborn-dvt mapped'
    WHEN patient_type_id=107 THEN 'ob/gyn'
    WHEN patient_type_id=26 THEN 'observation'
    WHEN patient_type_id=102 THEN 'other'
    WHEN patient_type_id=27 THEN 'outpatient'
    WHEN patient_type_id=103 THEN 'pediatrics'
    WHEN patient_type_id=134 THEN 'pre-admit'
    WHEN patient_type_id=91 THEN 'psychiatry'
    WHEN patient_type_id=105 THEN 'rehab/snf'
    WHEN patient_type_id=150 THEN 'no xref'
    WHEN patient_type_id=149 THEN 'unset'
    ELSE NULL
    END AS patient_type_description
, COUNT(patient_type_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY patient_type_id
;

--WHAT IS THE COUNT BY PATIENT TYPE? 
--Also well represented across levels. 
--How are the vars "Patient_TYpe " and "Patient_Class" related to each other? Is one more granular?
SELECT patient_type_id
, CASE
    WHEN patient_type_id=108 THEN 'community programs'
    WHEN patient_type_id=25 THEN 'emergency room'
    WHEN patient_type_id=106 THEN 'home health service'
    WHEN patient_type_id=28 THEN 'inpatient'
    WHEN patient_type_id=135 THEN 'lab'
    WHEN patient_type_id=104 THEN 'med/surg'
    WHEN patient_type_id=101 THEN 'newborn'
    WHEN patient_type_id=139 THEN 'newborn-dvt mapped'
    WHEN patient_type_id=107 THEN 'ob/gyn'
    WHEN patient_type_id=26 THEN 'observation'
    WHEN patient_type_id=102 THEN 'other'
    WHEN patient_type_id=27 THEN 'outpatient'
    WHEN patient_type_id=103 THEN 'pediatrics'
    WHEN patient_type_id=134 THEN 'pre-admit'
    WHEN patient_type_id=91 THEN 'psychiatry'
    WHEN patient_type_id=105 THEN 'rehab/snf'
    WHEN patient_type_id=150 THEN 'no xref'
    WHEN patient_type_id=149 THEN 'unset'
    ELSE NULL
    END AS patient_type_description
, COUNT(patient_type_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY patient_type_id
;

--ADMIT_SOURCE_ID and _TYPE: i.e. where was the member before their admit
SELECT 
admit_source_id
, CASE
    WHEN admit_source_id=22 THEN 'affiliated hospital referral'
    WHEN admit_source_id=87 THEN 'affiliated hospital transfer'
    WHEN admit_source_id=14 THEN 'affiliated outpatient referral'
    WHEN admit_source_id=20 THEN 'clinic specialty referral'
    WHEN admit_source_id=17 THEN 'emergency'
    WHEN admit_source_id=69 THEN 'home health service transfer'
    WHEN admit_source_id=19 THEN 'law'
    WHEN admit_source_id=21 THEN 'managed care referral'
    WHEN admit_source_id=121 THEN 'newborn'
    WHEN admit_source_id=68 THEN 'non affiliated hospital'
    WHEN admit_source_id=13 THEN 'non affiliated hospital referral'
    WHEN admit_source_id=67 THEN 'non affiliated hospital transfer'
    WHEN admit_source_id=88 THEN 'non affiliated hospital transfer'
    WHEN admit_source_id=23 THEN 'non affiliated outpatient referral'
    WHEN admit_source_id=16 THEN 'other'
    WHEN admit_source_id=122 THEN 'outpatient'
    WHEN admit_source_id=63 THEN 'outpatient transfer'
    WHEN admit_source_id=24 THEN 'physician referral'
    WHEN admit_source_id=64 THEN 'rehab/snf'
    WHEN admit_source_id=18 THEN 'rehab/snf referral'
    WHEN admit_source_id=15 THEN 'rehab/snf referral'
    WHEN admit_source_id=66 THEN 'rehab/snf transfer'
    WHEN admit_source_id=89 THEN 'self referral'
    WHEN admit_source_id=146 THEN 'No Xref'
    WHEN admit_source_id=145 THEN 'unset'
    ELSE NULL
    END AS admit_source_description
, COUNT(admit_source_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY admit_source_id
;

--what is the relationship between this field and others identifying visit type?
SELECT 
admit_type_id
, CASE
    WHEN admit_type_id=9 THEN 'emergency room'
    WHEN admit_type_id=137 THEN 'inpatient'
    WHEN admit_type_id=138 THEN 'lab'
    WHEN admit_type_id=7 THEN 'newborn'
    WHEN admit_type_id=11 THEN 'other'
    WHEN admit_type_id=10 THEN 'outpatient'
    WHEN admit_type_id=12 THEN 'rehab/snf'
    WHEN admit_type_id=90 THEN 'self referral'
    WHEN admit_type_id=8 THEN 'urgent care'
    WHEN admit_type_id=154 THEN 'No Xref'
    WHEN admit_type_id=153 THEN 'Unset'
    ELSE NULL
    END AS admit_type_description
, COUNT(admit_type_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY admit_type_id
;

--ADMIT_DATE: Frequency by Year
SELECT 
EXTRACT(YEAR FROM ADMIT_DATE) AS "Admit_Year"
, COUNT(EXTRACT(YEAR FROM ADMIT_DATE)) AS "Frequency"
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
GROUP BY EXTRACT(YEAR FROM ADMIT_DATE)
ORDER BY  EXTRACT(YEAR FROM ADMIT_DATE) DESC
;

--HOW MUCH DATA YESTERDAY? 4 DAYS AGO? 30 DAYS AGO? 
--NOTE THAT FUTURE DATES ARE NOT IN ERROR. As Per Tony Truong, some facilities enter dates a member is expected to arrive for a scheduled inpatient admission.
--Note the use of DATE below conforms with Ben's explanation here: https://stackoverflow.com/questions/20171768/using-sql-query-with-group-by-date
SELECT COUNT(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
--AND ADMIT_DATE IS NOT NULL
--AND TRUNC(ADMIT_DATE)='1-FEB-2018' --294
--AND TRUNC(ADMIT_DATE)='31-JAN-2018' --411
AND TRUNC(ADMIT_DATE)='1-JAN-2018' --796
ORDER BY ADMIT_DATE DESC
;
--AND TRUNC(ADMIT_DATE) BETWEEN DATE '1-FEB-2018' AND DATE '2-FEB-2018' 
--AND TRUNC(ADMIT_DATE) BETWEEN DATE '31-JAN-2018' AND DATE '1-JAN-2018' 
--AND TRUNC(ADMIT_DATE) BETWEEN DATE '1-JAN-2018' AND DATE '2-JAN-2018' 

--Daily admissions increase over time. Retrospective data entry? Some facilities add data later than others? Ask Tony Truong. 
--> see CREATED date field to figure this out
--30 dates for today 
--50 dates in the future (possibly in error --> no, some facilities are prospective. Though the 2020 year is probably an error)

SELECT 
TRUNC(ADMIT_DATE)
, Count(ADMIT_DATE) AS "Frequency Admit"
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
AND EXTRACT(YEAR FROM ADMIT_DATE) IN (2018)
GROUP BY TRUNC(ADMIT_DATE)
ORDER BY TRUNC(ADMIT_DATE) DESC
;

--DISCHARGE DATE
--Note for grouping by a date firled in ADT?eConnect: ts not clear what your table schema is, but I suspect that system_date is a datetime field, not a date field, this means that your grouping is being done incorrectly and also includes the time portion of the field.
--Note that there are no future dates here. 
--****ASK TONY TRUONG: IF A READMIT HAPPENS <24 HRS< IS IT RECLASSIFIED AS A CONTINUOUS ADMIT? That would be standard practice. COULD THIS EXPLAIN THE UNCERTAINTY IN ADMITS ABOUT 1 DAY OLD?
SELECT 
TRUNC(DISCHARGE_DATE)
, COUNT(DISCHARGE_DATE) AS "Frequency"
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
AND EXTRACT(YEAR FROM ADMIT_DATE) IN (2018)
GROUP BY TRUNC(DISCHARGE_DATE)
ORDER BY TRUNC(DISCHARGE_DATE) DESC
;

--DISCHARGE_DISPO_ID
--****ASK TONY TRUONG: These codes are from the data dictionary but most codes used for this field are not in the dictionary, and most codes in the dictionary are not used in this field. Need update dictionary?
SELECT 
DISCHARGE_DISPO_ID
, CASE
  WHEN DISCHARGE_DISPO_ID='1' THEN 'Home'
  WHEN DISCHARGE_DISPO_ID='2' THEN 'S/T Hospital'
  WHEN DISCHARGE_DISPO_ID='3' THEN	'SNF'
  WHEN DISCHARGE_DISPO_ID='4' THEN	'SNF'
  WHEN DISCHARGE_DISPO_ID='5' THEN	'Institution'
  WHEN DISCHARGE_DISPO_ID='6' THEN	'Home'
  WHEN DISCHARGE_DISPO_ID='7' THEN	'AMA'
  WHEN DISCHARGE_DISPO_ID='8' THEN	'Home'
  WHEN DISCHARGE_DISPO_ID='9' THEN	'Hospital'
  WHEN DISCHARGE_DISPO_ID='20' THEN	'Expired'
  WHEN DISCHARGE_DISPO_ID='30' THEN	'Hospital'
  WHEN DISCHARGE_DISPO_ID='40' THEN	'Expired'
  WHEN DISCHARGE_DISPO_ID='41' THEN	'Expired'
  WHEN DISCHARGE_DISPO_ID='42' THEN	'Expired'
  WHEN DISCHARGE_DISPO_ID='43' THEN	'Fed Hospital'
  WHEN DISCHARGE_DISPO_ID='50' THEN	'Hospice'
  WHEN DISCHARGE_DISPO_ID='51' THEN	'Hospice'
  WHEN DISCHARGE_DISPO_ID='61' THEN	'Hospital'
  WHEN DISCHARGE_DISPO_ID='62' THEN	'Transfer-Rehab Fac'
  WHEN DISCHARGE_DISPO_ID='64' THEN	'Transfer Nursing Facility/Custodial Care'
  WHEN DISCHARGE_DISPO_ID='99' THEN	'Other'
  WHEN DISCHARGE_DISPO_ID='UNS' THEN	'Unset'
  WHEN DISCHARGE_DISPO_ID='NX'	 THEN 'No Xref'
  ELSE NULL
  END AS Discharge_dispo_description
, COUNT(DISCHARGE_DISPO_ID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
AND EXTRACT (YEAR FROM DISCHARGE_DATE) IN (2017, 2018)
GROUP BY DISCHARGE_DISPO_ID
;

--discharge_lication_id
--TONY TRUONG: no id code explanation in the data dictionary. Freeform field? Lots of variety in the input.
SELECT DISTINCT DISCHARGE_LOCATION_ID
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
;

--ACCOUNT_NUMBER
--DIctionary says "The account number used to link ADT messages; typically PID.18." TONY TRUONG: please explain?
SELECT ACCOUNT_NUMBER
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--HOSPITAL_SERVICE_ID
--How many unique hospitals are in the database now?
SELECT COUNT(DISTINCT HOSPITAL_SERVICE_ID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
;
--51
--BUT HOW MANY HAVE DATA? TABLE OF ACTIVITY BY HOSPITAL ID
SELECT HOSPITAL_SERVICE_ID, COUNT(DISTINCT VISIT_GUID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
GROUP BY HOSPITAL_SERVICE_ID
ORDER BY COUNT(DISTINCT VISIT_GUID) ASC
;
--How many unique hospitals are active?
SELECT COUNT(DISTINCT HOSPITAL_SERVICE_ID)
FROM
(
  SELECT HOSPITAL_SERVICE_ID, COUNT(DISTINCT VISIT_GUID) AS ACTIVITY
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE ds_visit_type_id = 70 --INPATIENT
  GROUP BY HOSPITAL_SERVICE_ID
  ORDER BY COUNT(DISTINCT VISIT_GUID) ASC
) S
WHERE ACTIVITY >=100
;
--34

--SOURCE_FACILITY_ID
--How is this field different from HOSPITAL_SERVICE_ID? What about SERVICING_FACILITY_ID?
--from dictionary:
--hospital_service_id: SNC Hospital Serv. Coded from HL7 (PV1.10)  
--source_facility_id: Hospital Facility ID as sent to SNC, typically coded from MSH.4.
--servicing_facility_id: Servicing facility if sent, typically coded from PV1.39.
--ASK TONY TRUONG FOR DIFFERENCE
SELECT COUNT(DISTINCT SOURCE_FACILITY_ID)
FROM
(
  SELECT SOURCE_FACILITY_ID, COUNT(DISTINCT VISIT_GUID) AS ACTIVITY
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE ds_visit_type_id = 70 --INPATIENT
  GROUP BY SOURCE_FACILITY_ID
  ORDER BY COUNT(DISTINCT VISIT_GUID) ASC
) S
WHERE ACTIVITY >=100
;
--25, by contrast to the 34 for similar query using HOSPITAL_SERVICE_ID

SELECT COUNT(DISTINCT SERVICING_FACILITY_ID)
FROM
(
  SELECT SERVICING_FACILITY_ID, COUNT(DISTINCT VISIT_GUID) AS ACTIVITY
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE ds_visit_type_id = 70 --INPATIENT
  GROUP BY SERVICING_FACILITY_ID
  ORDER BY COUNT(DISTINCT VISIT_GUID) ASC
) S
WHERE ACTIVITY >=100
;
--31, by contrast to the 34 for similar query using HOSPITAL_SERVICE_ID

--LOCATION_POINT_OF_CARE
--DIctionary says "Location details from facility (PV1.3.1)." 
--Looks freeform and may be hard to use
SELECT LOCATION_POINT_OF_CARE
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--LOCATION_ROOM
--DIctionary says "Location details from facility (PV1.3.1)." 
--Looks freeform and may be hard to use
SELECT LOCATION_ROOM
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--LOCATION_BED
--DIctionary says "Location details from facility (PV1.3.1)." 
--Looks freeform and may be hard to use
SELECT LOCATION_BED
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--LOCATION_FACILITY
--DIctionary says "Location details from facility (PV1.3.1)." 
--Looks freeform and may be hard to use
SELECT LOCATION_FACILITY
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--LOCATION_BUILDING
--DIctionary says "Location details from facility (PV1.3.1)." 
--has many nulls
SELECT LOCATION_BUILDING
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;
--how many nulls?
SELECT LOCATION_BUILDING, COUNT(VISIT_GUID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY LOCATION_BUILDING
; 
--many nulls

--LOCATION_FLOOR
--DIctionary says "Location details from facility (PV1.3.1)." 
--Looks freeform and may be hard to use
SELECT LOCATION_BED
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;

--LOCATION_DESC
--DIctionary says "Location details from facility (PV1.3.1)." 
--many nulls
SELECT LOCATION_DESC
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;
--how many nulls?
SELECT LOCATION_DESC, COUNT(VISIT_GUID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY LOCATION_DESC
; 
--all nulls

--LOCATION_TYPE
--DIctionary says "Location details from facility (PV1.3.1)." 
--many nulls
SELECT LOCATION_TYPE
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ROWNUM <= 10
;
--how many nulls?
SELECT LOCATION_TYPE, COUNT(VISIT_GUID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY LOCATION_TYPE
; 
--many nulls

--DIAG_CODING_METHOD Primary Diagnosis; from DG1.2 or DG1.3.3.
SELECT diag_coding_method
from encounter.admit_dischrg_transf_data_snc
WHERE ROWNUM <=10;
--EVERYTHING IS NULL. iS THIS WHERE i SHOULD LEARN WHETHER, E.G., THE DIAG CODE IS ON OCD9 OR 10? ASK TONY TRUONG
SELECT DIAG_CODING_METHOD, COUNT(diag_coding_method)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY DIAG_CODING_METHOD
;

--DIAG_PRIORITY
SELECT diag_priority
from encounter.admit_dischrg_transf_data_snc
WHERE ROWNUM <=10;
--EVERYTHING IS NULL
SELECT diag_priority, COUNT(diag_priority)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY diag_priority
;

--DIAG_CODE
SELECT DIAG_CODE
from encounter.admit_dischrg_transf_data_snc
WHERE ROWNUM <=10;
--EVERYTHING IS NULL
SELECT DIAG_CODE, COUNT(DIAG_CODE)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY DIAG_CODE
;

--DIAG_TEXT
SELECT DIAG_TEXT
from encounter.admit_dischrg_transf_data_snc
WHERE ROWNUM <=10;
--EVERYTHING IS NULL
SELECT DIAG_TEXT, COUNT(DIAG_TEXT)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY DIAG_TEXT
;

--DIAG_TYPE
SELECT DIAG_TYPE
from encounter.admit_dischrg_transf_data_snc
WHERE ROWNUM <=10;
--EVERYTHING IS NULL
SELECT DIAG_TYPE, COUNT(DIAG_TYPE)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
GROUP BY DIAG_TYPE
;

--CREATED
--HOW MANY DAYS SEPARATE AN ADMISSION FROM A CREATION?
--WEIRD CAUSE INCLUDES FUTURE DATES AND POSSIBLY ALSO THE MOMENT OF GENESIS OF THE DATABASE
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(CREATED) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
) S
GROUP BY RECORDING_DELAY;

--SAME QUESTION BUT WITHIN THE PAST 3 MONTHS
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(CREATED) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
  WHERE TRUNC(ADMIT_DATE) BETWEEN TO_DATE('02-NOV-2017') AND TO_DATE('03-FEB-2018')
) S
GROUP BY RECORDING_DELAY
ORDER BY RECORDING_DELAY DESC;
--ask tony TRUONG: THIS DOESN'T CAPTURE THE DELAY I SEE WHEN I TRACK THE DATE THE ADMIT ITEM ACTUALLY APPEARS IN THE ECONNECT SYSTEM IN ON OUR END. IS THAT BECAUSE OF THE ADDED DELAY (AFTER ROW CREATION) IN UPDATING OUR END?

--LAST_MODIFIED
--HOW MANY DAYS SEPARATE AN ADMISSION FROM A last_MODIFIED DATE WITHIN THE PAST 3 MONTHS
--modifications fall off steeply over first 10 days, with vast majority modeifications happening on dayy 0 or 1
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(LAST_MODIFIED) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
  WHERE TRUNC(ADMIT_DATE) BETWEEN TO_DATE('02-NOV-2017') AND TO_DATE('03-FEB-2018')
) S
GROUP BY RECORDING_DELAY
ORDER BY RECORDING_DELAY DESC;

--DS_VISIT_STATUS_DATE
--ASK TONY TRUONG: I don't understand this field
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(DS_VISIT_STATUS_DATE) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
  WHERE TRUNC(ADMIT_DATE) BETWEEN TO_DATE('02-NOV-2017') AND TO_DATE('03-FEB-2018')
) S
GROUP BY RECORDING_DELAY
ORDER BY RECORDING_DELAY DESC;

--DISCHARGE_STATUS_DATE
--ASK TONY TRUONG: I don't understand this field
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(DISCHARGE_STATUS_DATE) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
  WHERE TRUNC(ADMIT_DATE) BETWEEN TO_DATE('02-NOV-2017') AND TO_DATE('03-FEB-2018')
) S
GROUP BY RECORDING_DELAY
ORDER BY RECORDING_DELAY DESC;

--LAST_TOUCH_DATE
--ASK TONY TRUONG: what's the difference between LAST_TOUCH_DATE and last_modeified?
SELECT RECORDING_DELAY, COUNT(RECORDING_DELAY)
FROM (
  SELECT TO_DATE(LAST_TOUCH_DATE) - TO_DATE(ADMIT_DATE) AS RECORDING_DELAY
  from encounter.admit_dischrg_transf_data_snc
  WHERE TRUNC(ADMIT_DATE) BETWEEN TO_DATE('02-NOV-2017') AND TO_DATE('03-FEB-2018')
) S
GROUP BY RECORDING_DELAY
ORDER BY RECORDING_DELAY DESC;