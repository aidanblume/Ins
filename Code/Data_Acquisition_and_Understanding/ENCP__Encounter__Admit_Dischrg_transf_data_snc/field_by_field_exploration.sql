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
    END AS patient_type_description
, COUNT(patient_type_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY patient_type_id
;

--ADMIT_SOURCE_ID and _TYPE: i.e. where was the member before their admit
SELECT admit_source_id
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
    END AS admit_source_description
, COUNT(admit_source_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY admit_source_id
;

--ADMIT_SOURCE_ID and _TYPE: i.e. where was the member before their admit
SELECT admit_source_id
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
    END AS admit_source_description
, COUNT(admit_source_id) AS Frequency
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --inpatient visits only. So result here should only be inpatient class. 
AND DS_VISIT_STATUS_ID = 75 
GROUP BY admit_source_id
;

--what is the relationship between this field and others identifying visit type?
SELECT admit_type_id
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

--HOW MUCH DATA YESTERDAY? 4 DAYS AGO? 30 DAYS AGO? HOW MUCH "IN FUTURE"/error?
SELECT COUNT(*)
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
AND ADMIT_DATE IS NOT NULL
AND ADMIT_DATE BETWEEN '29-JAN-18' AND '30-JAN-18'
ORDER BY ADMIT_DATE DESC
;
--226
--30 dates for today (possibly in error)
--50 dates in the future (clear error)


--RECENCY OF ADMIT DATA (run on 2/1)
SELECT *
FROM ENCOUNTER.ADMIT_DISCHRG_TRANSF_DATA_SNC
WHERE ds_visit_type_id = 70 --INPATIENT
AND ADMIT_DATE IS NOT NULL
--AND ADMIT_DATE BETWEEN '31-JAN-18' AND '01-FEB-18'
ORDER BY ADMIT_DATE DESC
;