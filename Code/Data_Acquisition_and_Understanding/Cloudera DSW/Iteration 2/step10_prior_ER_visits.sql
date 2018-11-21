/***
Title:              step10_prior_ER_visits
Description:        Add a count of ER visits within 6-month to the index hospitalization (for LACE). 
                    Merging priority: QNXT>CLM>ENC
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step9_postdischargeSNF 
Output:             nathalie.prjrea_step10_ER
Assumption:         No case logic because it is assumed that an admit date is uniquely tied to an ER visit (i.e. consecutive admit dates are distinct, and ER visits last no more than 24 hrs)
***/

drop table if exists nathalie.prjrea_step10_ER 
;

set max_row_size = 20mb
;

create table nathalie.prjrea_step10_ER 
as
select a.*, b.count_prior6m_er
from
nathalie.prjrea_step9_postdischargeSNF a 
left join
(
    select a.cin_no, a.adm_dt
        , sum(case  when (int_months_between(a.adm_dt, er.er_adm_dt) <= 6 and datediff(a.adm_dt, er.er_adm_dt) >= 0)
                    then 1 else 0 end) as count_prior6m_er
    from nathalie.prjrea_step9_postdischargeSNF a
    left join 
    (
        select case_id, cin_no, er_adm_dt
        from 
        ( --add row number
            select *
                , row_number() over(partition by cin_no, case_id, er_adm_dt order by source_table asc) as rownumber
            from
            (  -- union reports of ER events from 4 sources: claims_universe, & HOAP clm, enc, qnxt tables 
                select distinct claimid as case_id, carriermemid as cin_no, startdate as er_adm_dt
                    , 1 as source_table
                from SWAT.claims_universe
                where er_ind not in ('non_ER') -- field value has changed.  / avoidable ED algo. 
                
                union
            
                select distinct hdr.case_id, hdr.cin_no, admit_dt_clm as er_adm_dt --note that admit_dt_clm has uninformative hours: they're always 7 or 8 am
                    , 3 as source_table
                from hoap.clm_hdr_inpsnf hdr join hoap.clm_detail_inpsnf det 
                on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
                where substr(hdr.type_bill,1,2) in ('11','12', '13', '14') 
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
                and (hdr.pos_1 = '23' OR det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459'))
                and hdr.admit_dt_clm is not null
                and bp_code<>'TZGQ'

                union
                
                select distinct hdr.case_id, hdr.cin_no, hdr.admit_dt_clm as er_adm_dt 
                    , 4 as source_table
                from hoap.enc_hdr_inpsnf hdr join hoap.enc_detail_inpsnf det 
                on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
                where substr(type_bill,1,2) in ('11','12', '13', '14') 
                and (hdr.pos_1 = '23' OR det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459', '450', '451', '452', '453', '454', '455', '456', '457', '458', '459'))
                and hdr.admit_dt_clm is not null
                and bp_code<>'TZGQ'

            ) ALL_ER
        ) rownumber_added
        where rownumber = 1
    ) er
    on a.cin_no = er.cin_no
    group by a.cin_no, a.adm_dt
) b
on a.cin_no = b.cin_no and a.adm_dt = b.adm_dt
;
