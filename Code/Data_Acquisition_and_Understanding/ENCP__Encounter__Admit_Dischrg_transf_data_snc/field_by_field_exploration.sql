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

