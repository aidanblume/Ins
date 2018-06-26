/***
Title:              step5_ER
Description:        Add a count of ER visits within 6-month to the index hospitalization (for LACE). 
                    Merging priority: QNXT>CLM>ENC
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4d_SNF 
Output:             nathalie.prjrea_step5_ER
***/

create table nathalie.prjrea_step5_ER 
as
select a.*, b.count_prior6m_er
from
nathalie.prjrea_step4d_SNF a 
left join
(
    select a.cin_no, a.adm_dt
        , sum(case  when (datediff(a.adm_dt, er.er_adm_dt) <= 183 and datediff(a.adm_dt, er.er_adm_dt) >= 1)
                    then 1 else 0 end) as count_prior6m_er
    from nathalie.prjrea_step4d_SNF a
    left join 
    (
        select case_id, cin_no, er_adm_dt
        from 
        ( --add row number
            select *
                , row_number() over(partition by cin_no, case_id, er_adm_dt order by source_table asc) as rownumber
            from
            (  -- union reports of ER events from 3 sources: clm, enc, qnxt tables 
                select distinct case_id, cin_no, trunc(admit_dt_clm, 'DD') as er_adm_dt 
                    , 1 as source_table
                from HOAP.qnxt_hdr_inpsnf  
                where substr(type_bill,1,2) in ('11','12')
                -- Type of Bill Codes (Form Locator 4)
                -- REF: http://dhs.pa.gov/cs/groups/webcontent/documents/manual/s_001939.pdf
                -- INPATIENT ONLY:
                    -- First Digit
                        -- 1 Type of Facility – Hospital
                    -- Second Digit
                        -- 1 Bill Classification – Inpatient
                        -- '2'='Inpatient (Medicare Part B Only) / per Mary Q's claims universe
                    -- Third Digit
                        -- 0 Non Payment/Zero Claim
                        -- 1 Admit through Discharge Claim
                        -- 2 Interim – First Claim
                        -- 7 Replacement of Prior Claim
                        -- 8 Void/Cancel of Prior Claim
                -- OUTPATIENT ONLY:
                    -- First Digit
                        -- 1 Type of Facility – Hospital
                    -- Second Digit
                        -- 3 Bill Classification – Outpatient
                        -- 4 Bill Classification – Hospital Special Treatment Room
                    -- Third Digit
                        -- 0 Nonpayment/Zero Claim
                        -- 1 Admit through Discharge Claim
                        -- 7 Replacement of Prior Claim
                        -- 8 Void/Cancel of Prior Claim 
                and adm_type in ('1','2', '5') 
                -- Admission Type (Form Locator 14) 
                -- admission type on UB92, indicates the priority of the inpatient admission. 
                -- REF: http://dhs.pa.gov/cs/groups/webcontent/documents/manual/s_001939.pdf
                -- 1 Emergency Admission
                -- 2 Urgent Admission
                -- 3 Elective Admission
                -- 4 Newborn Admission
                -- 5 Trauma Admission (Emergency Admission)
                and admit_dt_clm is not null
                union
                select distinct hdr.case_id, hdr.cin_no, trunc(hdr.admit_dt_clm, 'DD') as er_adm_dt 
                    , 2 as source_table
                from hoap.clm_hdr_inpsnf hdr join hoap.clm_detail_inpsnf det 
                on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
                where substr(type_bill,1,2) in ('11','12')
                and det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457'
                    , '0458', '0459', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459')
                and hdr.admit_dt_clm is not null
                union
                select distinct hdr.case_id, hdr.cin_no, trunc(hdr.admit_dt_clm, 'DD') as er_adm_dt 
                    , 3 as source_table
                from hoap.enc_hdr_inpsnf hdr join hoap.enc_detail_inpsnf det 
                on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
                where substr(type_bill,1,2) in ('11','12')
                and det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457'
                    , '0458', '0459', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459')
                and hdr.admit_dt_clm is not null
            ) ALL_ER
        ) rownumber_added
        where rownumber = 1
    ) er
    on a.cin_no = er.cin_no
    group by a.cin_no, a.adm_dt
) b
on a.cin_no = b.cin_no and a.adm_dt = b.adm_dt
;
