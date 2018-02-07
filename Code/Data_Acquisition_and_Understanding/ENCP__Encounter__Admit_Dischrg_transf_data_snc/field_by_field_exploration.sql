/*
From Oracle SQL: ENCP database, ENCOUNTER schema, ADMIT_DISCHRG_TRANSF_DATA_SNC table

DESCRIPTION: Exploration of the eConnect/ADT database for the purpose of predicting readmission

AU: Nathalie Blume
PROJECT: Readmission Rate Reduction

ACTIONS: //WAIT UNTIL YOU SEE HIS ANALYTICS REPORT THIS WEEK
Contact Tony Truong to get answers to the following questions:
- Daily admissions increase over time. Retrospective data entry? Some facilities add data later than others? Get info on which factors affect early vs. on-time vs. late reporting of admissions, transfers and discharges
- IF A READMIT HAPPENS <24 HRS< IS IT RECLASSIFIED AS A CONTINUOUS ADMIT? 
- DISCHARGE_date: IF A READMIT HAPPENS <24 HRS< IS IT RECLASSIFIED AS A CONTINUOUS ADMIT? That would be standard practice. COULD THIS EXPLAIN THE UNCERTAINTY IN ADMITS ABOUT 1 DAY OLD?
- --DISCHARGE_DISPO_ID: These codes are from the data dictionary but most codes used for this field are not in the dictionary, and most codes in the dictionary are not used in this field. Need update dictionary?
- DISCHARGE_location_ID: no id code explanation in the data dictionary. Freeform field? Lots of variety in the input.
-ACCOUNT_NUMBER: DIctionary says "The account number used to link ADT messages; typically PID.18." TONY TRUONG: please explain?
-SOURCE_FACILITY_ID: How is this field different from HOSPITAL_SERVICE_ID? What about SERVICING_FACILITY_ID? from dictionary:
      hospital_service_id: SNC Hospital Serv. Coded from HL7 (PV1.10)  
      source_facility_id: Hospital Facility ID as sent to SNC, typically coded from MSH.4.
      servicing_facility_id: Servicing facility if sent, typically coded from PV1.39.
-diag_coding_method: --EVERYTHING IS NULL. iS THIS WHERE i SHOULD LEARN WHETHER, E.G., THE DIAG CODE IS ON OCD9 OR 10? 
- recoding_delay: THIS DOESN'T CAPTURE THE DELAY I SEE WHEN I TRACK THE DATE THE ADMIT ITEM ACTUALLY APPEARS IN THE ECONNECT SYSTEM IN ON OUR END. IS THAT BECAUSE OF THE ADDED DELAY (AFTER ROW CREATION) IN UPDATING OUR END?
- DS_VISIT_STATUS_DATE: I don't understand this field
- DISCHARGE_STATUS_DATE: I don't understand this field
--LAST_TOUCH_DATE: what's the difference between LAST_TOUCH_DATE and last_modified?
--there are duplicates. Why? [show code]
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

--discharge_location_id
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

-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------

/*

What data here are useful for a readmission model?

demographics: --> age (DOB)

cur_LOB --> how the member signed up & who is financially responsible may impart readmission

what service admitted the patient (ER, inpatient, psychiatry, etc.) --> urgent services may offer lesser quality care; other levels of the factor may be associated with diagnosis.
    - serviciing_facility_id
    - ds_visit_type_id
    - patient_class_id
    - patient_type_id
    + location_point_of_service
    + hospital_service_id
    
phone & phone_type --> may indicate both level of social support (proving someone else's phone number) and fnuctional level (having a phone)

admitting_physician
attending_aa
attending_id
    --> i could compute the number of admissions for that person per unit of time to show over-burden and hence effect on quality of care
    --> i could also detect patterns where certain physicians have more readmits
    
diag_priority

Dx? --> diag_code; diag_text? admit_reason_id?  admit_type_id? + diag_type

source facility --> source_facility_id; admit_source_id; 

admit_date

discharge_date
discharge_dispo
discharge_location

readmit <24hrs? --> look at dates: created, admit_date; last_modified; visit_status_date; discharge_status_date; last_touch_date.



-- diagnoses
-- day and time of day admitted
-- day and time of day discharged
-- transfers as source or endpoint
-- can I detect the presence of a <24 hour discharge taht would be invisible in billing records?
-- link to demographic data for the member
-- type of facility
-- type of facility/situation from which the patient was admitted
-- type of facility/situation to which the patient was discharged/transfered
-- language (60% populated)

*/

--DEMOGRAPHICS
--negative ages? only if you write a bad timedif query in oracle. 
SELECT 
 FLOOR(MONTHS_BETWEEN(ADMIT_DATE, DOB) / 12) AS age
, FLOOR(MONTHS_BETWEEN(TO_DATE(ADMIT_DATE), TO_DATE(DOB)) / 12) AS badage
, TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) AS age2
, ADMIT_DATE, DOB
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE TO_DATE(ADMIT_DATE) BETWEEN '1-JAN-2018' AND '1-FEB-2018'
AND FLOOR(MONTHS_BETWEEN(TO_DATE(ADMIT_DATE), TO_DATE(DOB)) / 12) < 0
AND ROWNUM <= 10
;

--age distribution over 1 month, 1 month ago
SELECT TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) AS age, count(TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12)) AS frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE TO_DATE(ADMIT_DATE) BETWEEN '1-JAN-2018' AND '1-FEB-2018'
GROUP BY TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12)
ORDER BY AGE
;

--by age group (decades) over 1 month, 1 month ago
SELECT agegroup, count(agegroup) AS frequency
FROM
  (
  SELECT 
    CASE
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <10 THEN '0-9'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <20 THEN '10-19'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <30 THEN '20-29'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <40 THEN '30-39'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <50 THEN '40-49'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <60 THEN '50-59'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <70 THEN '60-69'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <80 THEN '70-79'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <90 THEN '80-89'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <100 THEN '90-99'
    ELSE 'older'
    END AS agegroup
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE TO_DATE(ADMIT_DATE) BETWEEN '1-JAN-2018' AND '1-FEB-2018'
  ) S
GROUP BY agegroup
ORDER BY agegroup
;

--age distribution 1 day, 1 week ago (1st pass at seeing age differential in recentcy of the data)
SELECT agegroup, count(agegroup) AS frequency
FROM
  (
  SELECT 
    CASE
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <10 THEN '0-9'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <20 THEN '10-19'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <30 THEN '20-29'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <40 THEN '30-39'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <50 THEN '40-49'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <60 THEN '50-59'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <70 THEN '60-69'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <80 THEN '70-79'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <90 THEN '80-89'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <100 THEN '90-99'
    ELSE 'older'
    END AS agegroup
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE TO_DATE(ADMIT_DATE) BETWEEN '28-JAN-2018' AND '29-JAN-2018'
  ) S
GROUP BY agegroup
ORDER BY agegroup
;
--age distribution yesterday (1st pass at seeing age differential in recentcy of the data)
SELECT agegroup, count(agegroup) AS frequency
FROM
  (
  SELECT 
    CASE
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <10 THEN '0-9'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <20 THEN '10-19'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <30 THEN '20-29'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <40 THEN '30-39'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <50 THEN '40-49'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <60 THEN '50-59'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <70 THEN '60-69'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <80 THEN '70-79'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <90 THEN '80-89'
    WHEN TRUNC(MONTHS_BETWEEN(ADMIT_DATE, DOB)/12) <100 THEN '90-99'
    ELSE 'older'
    END AS agegroup
  FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
  WHERE TO_DATE(ADMIT_DATE) BETWEEN '4-FEB-2018' AND '5-FEB-2018'
  ) S
GROUP BY agegroup
ORDER BY agegroup
;
--growth
0-9	    3.215189873   --- may indicate that births are recorded more quickly (less likely to be an inpatient emergency)
10-19	  2.746987952   --- youths' admissions are reported quickest (least growth after 1st day until presumed ceiling is reached)
20-29	  3.92481203
30-39	  4.540540541   --- interesting that middle age is associated with later recording of event in the eConnect EHR
40-49	  4.710144928
50-59	  4.848214286
60-69	  3.605769231
70-79	  5.466666667   --- oldest members are recorded latest. To do with <24 hour readmission?
80-89	  6.5
90-99	  5.2

--LINE OF BUSINESS
SELECT cur_lob, COUNT(CUR_LOB)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE TO_DATE(ADMIT_DATE) BETWEEN '1-JAN-2017' AND '5-FEB-2018'
GROUP BY CUR_LOB
;
/*
HKID	43        ---> healthy kids LA
CFST	18553     ---> ?
BCSC	19941     ---> ?
HBEX	9079      ---> California Health Benefits Exchange (also known as “Health Insurance Marketplace” by DHHS and “Covered California”
KAIS	1069      ---> ?
COMM	804       ---> ?
CMC	25839       ---> cal mediConnect
MCLA	1168606   ---> direct medi-cal product line
PASC	11867     ---> homecare workers who meet the PASC eligibility requirement for health coverage
--with time filter
HKID	4
BCSC	10045
CFST	9646
HBEX	6785
KAIS	676
CMC	16333
MCLA	764080
PASC	8247
*/

/*
what service admitted the patient (ER, inpatient, psychiatry, etc.) --> urgent services may offer lesser quality care; other levels of the factor may be associated with diagnosis.
    - ds_visit_type_id
    - patient_class_id
    - patient_type_id
    + location_point_of_service
*/

SELECT DISTINCT ds_visit_type_id 
--SELECT COUNT(DISTINCT ds_visit_type_id) --2
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 70=iNPATIENT; 72=ER  

--SELECT DISTINCT patient_class_id 
SELECT COUNT(DISTINCT patient_class_id) --2
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 59= ER; 86= outpatient; 61= pre; 62=inpatient
--INPATIENTS can be inpatient or pre; ER can be er, pre or outpatient
SELECT DISTINCT DS_VISIT_TYPE_ID, PATIENT_CLASS_ID
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
ORDER BY ds_visit_type_id, patient_class_id
; 

SELECT DISTINCT patient_type_id 
--SELECT COUNT(DISTINCT patient_type_id) --2
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 10 types. Covers community programs, er, home health svc, inpatient, lab, med/surg, newborn, etc. See dictionary.
SELECT DISTINCT DS_VISIT_TYPE_ID, patient_type_id
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
ORDER BY ds_visit_type_id, patient_type_id
; 

SELECT DISTINCT location_point_of_care 
--SELECT COUNT(DISTINCT location_point_of_care) --2
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- freeform


/*
which facility?    
*/
--SELECT DISTINCT SERVICING_FACILITY_ID 
SELECT COUNT(DISTINCT SERVICING_FACILITY_ID)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 3-DIGIT NUMERIC CODE; 34 OF THEM

--hospital_service_id????
SELECT DISTINCT hospital_service_id 
--SELECT COUNT(DISTINCT hospital_service_id)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 3-DIGIT NUMERIC CODE; 51 OF THEM
/***********************TONY: WHAT IS THIS???? more specific than facility/hospital***************

/*
source facility --> source_facility_id; admit_source_id; 
*/
SELECT distinct source_facility_id
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 3 digit codes
SELECT count(distinct source_facility_id)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 32 of them
--
SELECT distinct admit_source_id
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 3 digit codes
SELECT count(distinct admit_source_id)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- 22 of them

/*    
phone & phone_type --> may indicate both level of social support (proving someone else's phone number) and fnuctional level (having a phone)
*/
SELECT COUNT(*) 
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE PHONE IS NOT NULL
; --1254991
SELECT COUNT(*) 
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE PHONE IS NULL
; --810

SELECT DISTINCT(phone_type)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
;
--can use this to indicate temporary (DATA) or emergency phone, possibly?
SELECT phone_type, count(phone_type)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
group by phone_type
;
-- very few "emergency" or "data". Biggest difference is between H(home) & a bunch of things that mean "mobile", but even so the intention behindthese distinctions is unclear.

/*
admitting_physician
attending_aa
attending_id
    --> i could compute the number of admissions for that person per unit of time to show over-burden and hence effect on quality of care
    --> i could also detect patterns where certain physicians have more readmits
*/
SELECT count(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where admitting_physician is not null
; --720,378
SELECT count(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where admitting_physician is null
; --535423 -- surbrisingly high number of null entries
--
SELECT count(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where attending_aa is null
; --785,261
--
SELECT count(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where attending_id is null
; --68,943 --> this is a smaller number than fields attending_aa and admitting_physician
--what about using info from any of the 3 fields? What's still null -- i.e. null across all 3 fields?
SELECT count(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where attending_id is null
and attending_aa is null
and admitting_physician is null
; --62,306


/* 
diag_priority
*/
SELECT distinct diag_priority
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; -- null. This field is not usefull at all


/*
Dx? --> diag_code; diag_text? admit_reason_id?  admit_type_id? + diag_type
*/
SELECT distinct diag_code
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; --null
SELECT distinct diag_text
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; --null

SELECT count(distinct admit_reason_id)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; --THIS HAS INFO********************************************************* may require text analysis
SELECT admit_reason_id, count(admit_reason_id)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
group by admit_reason_id
; 
---
SELECT distinct admit_type_id
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
; --a3 digit codes: Ref is in the DATA DICTIONARY.
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
GROUP BY admit_type_id
ORDER BY admit_type_id
;

/*
admit_date

discharge_date
discharge_dispo
discharge_location

readmit <24hrs? --> look at dates: created, admit_date; last_modified; visit_status_date; discharge_status_date; last_touch_date.
*/

SELECT admit_date, count(admit_date)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
group by admit_reason_id
; 
/*
--can i create a temporary table? NO
create global temporary table myTable
on commit preserve rows
as select * from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where rownum <= 10; 

--can I create a view? NO
create view myView 
as select * from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where rownum <= 10; 
*/


-- Add to each row the number of days since last inpatient service, allowing NULL when the service is the 1st for enrollee in time period of query.
--!!! I am having a hard time with negative date differences datediff. Resolved in the past with months_between, but the analog for days is not available
--!!! ABANDON QUERY BELOW -- duplicate enries are messing it up. Need to filter first. Also, anchor search to index, not readmission; it's less confusing.
select --view count of readmissions by amount of time lapsed since index admission
readmission_delay
, count(readmission_delay) as frequency
from
  (
  SELECT case
  when days_since_last_service is null then 'no readmission in time frame'
  when days_since_last_service <0 then 'less than 0'
  when days_since_last_service <8 then '0-7'
  when days_since_last_service <15 then '8-14'
  when days_since_last_service <22 then '15-21'
  when days_since_last_service <31 then '22-30'
  else 'more than 30'
  end as readmission_delay
  , readmit_date
  , enddate_of_last_service
  , days_since_last_service
  FROM
    (
    
    
          --BEGINING OF SUBQUERY "FLAG QUERY"
          SELECT --create a new table with rows tha capture index admissions in yr=2017 and the next admission for that patient in yr= 2017 or 2018
          A.row#
          , case 
            when A.member_id = B.member_id then A.member_id 
            else null
            end as readmit_memberid
          , case 
            when A.member_id = B.member_id then A.admit_date 
            else null
            end as readmit_date
          , case --
            when A.member_id = B.member_id then A.discharge_date 
            else null
            end as readmit_discharge_date
          , case 
            when A.member_id = B.member_id then A.HOSPITAL_SERVICE_ID 
            else null
            end as readmit_facility
          , case 
            when A.member_id = B.member_id then A.visit_guid 
            else null
            end as readmit_visitID --
          , B.discharge_date AS indexadmit_discharge_date
          , B.member_id as indexadmit_memberid
          ,CASE
            when A.member_id = B.member_id THEN (trunc(A.ADMIT_DATE) - trunc(B.discharge_date)) 
            ELSE NULL
          END AS days_since_last_service
          FROM 
          (
                  SELECT --add row number to basic table
                  ROW_NUMBER() OVER(ORDER BY member_id, admit_date ASC) AS row#
                  , HOSPITAL_SERVICE_ID, admit_date, discharge_date, member_id, visit_guid
                  FROM 
                  ( --get a basic table of admissions data where A. table is the readmission stay
                          select distinct HOSPITAL_SERVICE_ID, visit_guid, member_id, admit_date, discharge_date, ds_visit_type_id
                          from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
                          WHERE ds_visit_type_id = 70 AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
                  )
            ) A 
            INNER JOIN 
            (
                  SELECT --add row number to basic table
                  ROW_NUMBER() OVER(ORDER BY member_id, admit_date ASC) AS row#
                  , HOSPITAL_SERVICE_ID, admit_date, discharge_date, member_id, visit_guid
                  FROM 
                  ( -- get a basic table of admissions data where B. table is the index admission
                          select distinct HOSPITAL_SERVICE_ID, visit_guid, member_id, admit_date, discharge_date, ds_visit_type_id
                          from ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
                          WHERE ds_visit_type_id = 70 AND EXTRACT(YEAR FROM ADMIT_DATE) in ('2017', '2018')
                  )
            ) B 
            ON B.row# = A.row# - 1
            --end of subquery "FLAG_QUERY"
            
            
            
        )
        --GROUP BY DAYS_SINCE_LAST_SERVICE
        ORDER BY DAYS_SINCE_LAST_SERVICE
  )
group by readmission_delay
order by readmission_delay
;

/*
What about time? USE TRUNC?? still leaves 105 <0 and no null
*/

select
admit_date, discharge_date, trunc(discharge_date) - trunc(admit_date) as timedelay
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
where (trunc(aDMIT_DATE) - trunc(discharge_date)) <0
order by (trunc(aDMIT_DATE) - trunc(discharge_date)) asc
;





/*
-- diagnoses
-- day and time of day admitted
-- day and time of day discharged
-- transfers as source or endpoint
-- can I detect the presence of a <24 hour discharge taht would be invisible in billing records?
-- link to demographic data for the member
-- type of facility
-- type of facility/situation from which the patient was admitted
-- type of facility/situation to which the patient was discharged/transfered
-- language (60% populated)

*/
