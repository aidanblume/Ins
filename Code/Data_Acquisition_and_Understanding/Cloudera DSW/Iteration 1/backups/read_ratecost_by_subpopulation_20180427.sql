/***
20180414

- merge ER min(adm_dt) and ER total_days into case file by aggregating by case_id, cin_no
- bring in the from_ER from the 3 case tables [field name needs to be included]
- starting with njb_analytic_set, find adm_dt and do new sql query on the [what table has ER visits?] and count distinct ER visits per cin_no over 6 months period ending in adm_dt for case

NJB
- descriptive stats --> Brandon
- comorbidity predictors "tkcom"
***/

drop table if exists nathalie.njb_a;
drop table if exists nathalie.njb_analytic_set;
drop table if exists nathalie.njb_demographics_added;
drop table if exists nathalie.njb_hedis;
drop table if exists nathalie.njb_hedis_step3;
drop table if exists nathalie.njb_labeled_as_readmits;
drop table if exists nathalie.njb_labeled_outcomes;
drop table if exists nathalie.njb_transfers_absorbed;
drop table if exists nathalie.njb_unique_cases;
drop table if exists nathalie.njb_unique_cases_2;
drop table if exists nathalie.QS_UNIQUE_HDR_DET;


/*
UNIQUE_CASES
*/

/*
--Select all unique cases across QNXT, CLM and ENC, deduplicating with priority QNXT>CLM>ENC. 
--Join with MEMMO information about LOB
--Contains on separate rows nitial admits and transfers (continuous stays). These will be unified into single stays on a subsequent step.
--Top-level select has : row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber
*/

--TK QING: CAN YOU ONLY INCLUDE FACILITY CODE TYPE 1
--Qing: add inpatient hospitalization conditions
--Per HOA dictionary: TYPE_BILL = ‘facilitycode’ || ‘billclasscodev’ || ‘frequencycode’
--Per Mary: 
--facilitycode: '1'='Hospital'
--billclasscode: '1'='Inpatient', '2'='Inpatient (Medicare Part B Only)''
--rev_cd: '0450'='EMERG ROOM', '0451'='ER/EMTALA','0452'='ER/BEYOND EMTALA','0456'='URGENT CARE','0459'='OTHER EMERG ROOM'


/*
CREATES ER FLAG 
[No longer needed since ER days are calculated for 6-month prior to Index Admission later]
*/
/*
create table NATHALIE.QS_UNIQUE_HDR_DET as
select cin_no, case_id, cl_id, type_bill, dos_1, admit_dt_clm, disch_dt_clm, srv_cat, from_er, ER_dos_1, Days_ER, source_table
    , row_number() over(order by cin_no, admit_dt_clm, disch_dt_clm) as rownumber
from (
select *
from (
select cin_no, case_id, cl_id, type_bill, dos_1, admit_dt_clm, disch_dt_clm, srv_cat, from_er, ER_dos_1, Days_ER, source_table
    , row_number() over(partition by cin_no, admit_dt_clm, disch_dt_clm order by source_table asc, case_id desc) as rownumber
from (
-- TK Qing finds QNXT has no ER rev_cd
    select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.type_bill, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm, hdr.srv_cat, hdr.from_er
--        , min(case when det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459') 
        , case when hdr.adm_type in ('1','2') then hdr.admit_dt_clm else null end as ER_dos_1
        , case when hdr.adm_type in ('1','2') then datediff(hdr.disch_dt_clm, hdr.admit_dt_clm) else 0 end as Days_ER
        , 1 as source_table
    from `hoap`.`qnxt_hdr_inpsnf` hdr
    where substr(hdr.type_bill,1,2) in ('11','12')
    union
    select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.type_bill, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm, hdr.srv_cat, hdr.from_er
        , min(case when det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459') 
            then det.dos else null end) as ER_dos_1
        , sum(case when det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459') 
            and datediff(det.dos, hdr.admit_dt_clm)>=0 and datediff(hdr.disch_dt_clm, det.thru_dt)>=0 
            then datediff(det.thru_dt, det.dos)
            else 0 end) as Days_ER
        , 2 as source_table
    from `hoap`.`clm_hdr_inpsnf` hdr join `hoap`.`clm_detail_inpsnf` det 
    on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
    where substr(type_bill,1,2) in ('11','12')
    group by hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.type_bill, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm, hdr.srv_cat, hdr.from_er
    union
    select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.type_bill, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm, hdr.srv_cat, hdr.from_er
        , min(case when det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459') 
            then det.dos else null end) as ER_dos_1
        , sum(case when det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459') 
            and datediff(det.dos, hdr.admit_dt_clm)>=0 and datediff(hdr.disch_dt_clm, det.thru_dt)>=0 
            then datediff(det.thru_dt, det.dos)
            else 0 end) as Days_ER
        , 3 as source_table
    from `hoap`.`enc_hdr_inpsnf` hdr join `hoap`.`enc_detail_inpsnf` det 
    on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
    where substr(type_bill,1,2) in ('11','12')
    group by hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.type_bill, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm, hdr.srv_cat, hdr.from_er
    ) AS ALL_CASES
order by cin_no, admit_dt_clm
) ALL_CASES_PARTITIONED
where rownumber =  1
) as UNIQUE_CASES
--where year(dos_1)=2017
;
*/

/*
UNIQUE_CASES then UNIQUE_CASES_2
with HEDIS flags
*/
drop table if exists NATHALIE.NJB_UNIQUE_CASES;

create table NATHALIE.NJB_UNIQUE_CASES 
as
select 
    case_id, adm_dt, dis_dt, UNIQUE_CASES.cin_no, member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , severity, aprdrg, dis_status, provider, paid_amt_case, from_er, source_table
    , cur_pcp, cur_site_no, product_code, segment, yearmth
    , row_number() over (order by UNIQUE_CASES.cin_no asc, adm_dt asc, dis_dt asc) as rownumber
from
(--select unique tupple (cin_no, admi_dt, dis_dt) tupple with priority QNXT>CLM>ENC
    select *
    from
    ( --add number rows inside partitions where each partition is a unique (cin_no, admi_dt, dis_dt) tupple
        select case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider, paid_amt_case, from_er, source_table
        , row_number() over(partition by cin_no, adm_dt, dis_dt order by source_table asc, case_id desc) as rownumber
        from
        ( -- union of cases across 3 data tables: qnxt, clm, enc
            select case_id, adm_dt, dis_dt, cin_no, member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity, aprdrg, dis_status, provider, paid_amt_case, from_er
            , 1 as source_table
            --from `hoap`.`tmp_QNXT_CASE_INPSNF`
            from `hoap`.`QNXT_CASE_INPSNF`
            where srv_cat = '01ip_a'
            union
            select case_id, adm_dt, dis_dt, cin_no, member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity, aprdrg, dis_status, provider, paid_amt_case, from_er
            , 2 as source_table
            --from `hoap`.`tmp_clm_case_inpsnf`
            from `hoap`.`clm_case_inpsnf`
            where srv_cat = '01ip_a'
            union
            select case_id, adm_dt, dis_dt, cin_no, member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity, aprdrg, dis_status, provider, null as paid_amt_case, from_er
            , 3 as source_table
            --from `hoap`.`tmp_ENC_CASE_INPSNF`
            from `hoap`.`ENC_CASE_INPSNF`
            where srv_cat = '01ip_a'
       ) AS ALL_CASES
        order by cin_no, adm_dt, dis_dt
    ) ALL_CASES_PARTITIONED
    where rownumber =  1
) as UNIQUE_CASES  
left join
( -- member information from MEMMO table
    select cin_no, cur_pcp, cur_site_no, product_code, segment, yearmth
    from 
    ( -- rank identical cin_no by recncy in order to unique rows by cin_no
        select cin_no, pcp as cur_pcp, site_no as cur_site_no, product_code, segment, yearmth
        , row_number() over(partition by cin_no order by yearmth desc) as rownumber
        --from `HOAP`.`tmp_memmo`
        from `HOAP`.`memmo`
    ) as M
    where rownumber = 1
) as MEMMO
on UNIQUE_CASES.cin_no = MEMMO.cin_no
;
--927051 (member_no, adm_dt, dis_dt)
--882221 (cin_no, adm_dt, dis_dt)

-- Check duplications
select cin_no, adm_dt, dis_dt, count(*)
from NATHALIE.NJB_UNIQUE_CASES
--where year(adm_dt) = 2017
group by cin_no, adm_dt, dis_dt
having count(*)>1
order by count(*) desc;

                /*
                SYNTHETIC DATA 
                */
                /*
                create view nathalie.njb_synthetic_unique_cases as
                select
                    case_id
                    , case 
                        when (digit in (0, 5)) then cast('2017-06-01' as timestamp)
                        when (digit in (1, 6)) then cast('2017-06-06' as timestamp)
                        when (digit in (2, 7)) then cast('2017-06-11' as timestamp)
                        when (digit in (3, 8)) then cast('2017-06-21' as timestamp)
                        else cast('2017-06-26' as timestamp)
                      end as adm_dt
                    , case 
                        when (digit in (0, 5)) then cast('2017-06-05' as timestamp)
                        when (digit in (1, 6)) then cast('2017-06-08' as timestamp)
                        when (digit in (2, 7)) then cast('2017-06-20' as timestamp)
                        when (digit in (3, 8)) then cast('2017-06-21' as timestamp)
                        else cast('2017-07-15' as timestamp)
                      end as dis_dt
                    , cin_no, member_no
                    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
                    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
                    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
                    , severity, aprdrg, dis_status, provider, paid_amt_case, source_table
                    , cur_pcp, cur_site_no, product_code, segment, yearmth
                    , rownumber
                    , digit
                from
                (
                    select
                        case_id
                        , cin_no, member_no
                        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
                        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
                        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
                        , severity, aprdrg, dis_status, provider, paid_amt_case, source_table
                        , cur_pcp, cur_site_no, product_code, segment, yearmth
                        , rownumber
                        --, digit = right(rownumber, 1)
                        , case 
                            when rownumber < 10 then rownumber
                            when rownumber between 10 and 99 then cast(substring(cast(rownumber as varchar(10)), 2, 1) as int) 
                            else cast(substring(cast(rownumber as varchar(10)), 2, 1) as int) 
                          end as digit
                    from nathalie.njb_unique_cases
                    limit 100
                ) as Subquery
                ;
                */

/*
QING work begin
*/
-- Dedup flatfile.hedis_readmission_value_sets_2016 based on priority provided below if the same code has multiple value_set_name
-- Need to ensure that each code is unique in this table to avoid double counting
drop table nathalie.hedis_readmission_value_sets_2016_dedup;

create table nathalie.hedis_readmission_value_sets_2016_dedup as
select code, value_set_name
        , case when value_set_name in ('Pregnancy', 'Perinatal Conditions') then 1 else 0 end as Pregnancy
        , case when value_set_name in ('Chemotherapy') then 1 else 0 end as Chemo
        , case when value_set_name in ('Rehabilitation') then 1 else 0 end as Rehab
        , case when value_set_name in ('Transplant') then 1 else 0 end as Transplant
from (  select code, value_set_name, row_number() over(partition by code order by code asc, seq asc) as rownumber
        from (  select code
                    , value_set_name
                    , case when value_set_name in ('Pregnancy', 'Perinatal Conditions') then 'A'
                           when value_set_name in ('Chemotherapy') then 'B'
                           when value_set_name in ('Rehabilitation') then 'C'
                           when value_set_name in ('Transplant') then 'D'
                           when value_set_name in ('Potentially Planned Procedures') then 'E'
                           when value_set_name in ('Acute Condition') then 'F'
                           when value_set_name in ('Cancer Treatment') then 'G'
                           else 'Z' end as seq
                from flatfile.hedis_readmission_value_sets_2016
                where code_system in ('ICD9CM', 'ICD10CM', 'ICD9PCS', 'ICD10PCS')
                order by code, seq
                ) ALL_CASES_PARTITIONED
        ) UNIQUE
where rownumber=1;

-- HEDIS dummies creation using the deduped code lookup (not the most concise way, but works for now)
-- Alternatives are: 1) group_concat(concat('"',code,'"'),','), or 2) find_in_set(<string>,<string list>)
drop table nathalie.njb_unique_cases_2;

create table nathalie.njb_unique_cases_2 as
select a.*
    , (case when b1.Pregnancy is null then 0 else b1.Pregnancy end) + (case when b2.Pregnancy is null then 0 else b2.Pregnancy end)
    + (case when b3.Pregnancy is null then 0 else b3.Pregnancy end) + (case when b4.Pregnancy is null then 0 else b4.Pregnancy end)
    + (case when b5.Pregnancy is null then 0 else b5.Pregnancy end) + (case when b6.Pregnancy is null then 0 else b6.Pregnancy end)
    + (case when b7.Pregnancy is null then 0 else b7.Pregnancy end) + (case when b8.Pregnancy is null then 0 else b8.Pregnancy end)
    + (case when b9.Pregnancy is null then 0 else b9.Pregnancy end) + (case when b10.Pregnancy is null then 0 else b10.Pregnancy end)
    + (case when b11.Pregnancy is null then 0 else b11.Pregnancy end) + (case when b12.Pregnancy is null then 0 else b12.Pregnancy end)
    + (case when b13.Pregnancy is null then 0 else b13.Pregnancy end) + (case when b14.Pregnancy is null then 0 else b14.Pregnancy end)
    + (case when b15.Pregnancy is null then 0 else b15.Pregnancy end) + (case when b16.Pregnancy is null then 0 else b16.Pregnancy end)
    + (case when b17.Pregnancy is null then 0 else b17.Pregnancy end) + (case when b18.Pregnancy is null then 0 else b18.Pregnancy end)
    + (case when b19.Pregnancy is null then 0 else b19.Pregnancy end) + (case when b20.Pregnancy is null then 0 else b20.Pregnancy end)
    + (case when p1.Pregnancy is null then 0 else p1.Pregnancy end) + (case when p2.Pregnancy is null then 0 else p2.Pregnancy end)
    + (case when p3.Pregnancy is null then 0 else p3.Pregnancy end) + (case when p4.Pregnancy is null then 0 else p4.Pregnancy end)
    + (case when p5.Pregnancy is null then 0 else p5.Pregnancy end) + (case when p6.Pregnancy is null then 0 else p6.Pregnancy end)
    + (case when p7.Pregnancy is null then 0 else p7.Pregnancy end) + (case when p8.Pregnancy is null then 0 else p8.Pregnancy end)
    + (case when p9.Pregnancy is null then 0 else p9.Pregnancy end) + (case when p10.Pregnancy is null then 0 else p10.Pregnancy end)
    as Pregnancy
    , (case when b1.Chemo is null then 0 else b1.Chemo end) + (case when b2.Chemo is null then 0 else b2.Chemo end)
    + (case when b3.Chemo is null then 0 else b3.Chemo end) + (case when b4.Chemo is null then 0 else b4.Chemo end)
    + (case when b5.Chemo is null then 0 else b5.Chemo end) + (case when b6.Chemo is null then 0 else b6.Chemo end)
    + (case when b7.Chemo is null then 0 else b7.Chemo end) + (case when b8.Chemo is null then 0 else b8.Chemo end)
    + (case when b9.Chemo is null then 0 else b9.Chemo end) + (case when b10.Chemo is null then 0 else b10.Chemo end)
    + (case when b11.Chemo is null then 0 else b11.Chemo end) + (case when b12.Chemo is null then 0 else b12.Chemo end)
    + (case when b13.Chemo is null then 0 else b13.Chemo end) + (case when b14.Chemo is null then 0 else b14.Chemo end)
    + (case when b15.Chemo is null then 0 else b15.Chemo end) + (case when b16.Chemo is null then 0 else b16.Chemo end)
    + (case when b17.Chemo is null then 0 else b17.Chemo end) + (case when b18.Chemo is null then 0 else b18.Chemo end)
    + (case when b19.Chemo is null then 0 else b19.Chemo end) + (case when b20.Chemo is null then 0 else b20.Chemo end)
    + (case when p1.Chemo is null then 0 else p1.Chemo end) + (case when p2.Chemo is null then 0 else p2.Chemo end)
    + (case when p3.Chemo is null then 0 else p3.Chemo end) + (case when p4.Chemo is null then 0 else p4.Chemo end)
    + (case when p5.Chemo is null then 0 else p5.Chemo end) + (case when p6.Chemo is null then 0 else p6.Chemo end)
    + (case when p7.Chemo is null then 0 else p7.Chemo end) + (case when p8.Chemo is null then 0 else p8.Chemo end)
    + (case when p9.Chemo is null then 0 else p9.Chemo end) + (case when p10.Chemo is null then 0 else p10.Chemo end)
    as Chemo
    , (case when b1.Rehab is null then 0 else b1.Rehab end) + (case when b2.Rehab is null then 0 else b2.Rehab end)
    + (case when b3.Rehab is null then 0 else b3.Rehab end) + (case when b4.Rehab is null then 0 else b4.Rehab end)
    + (case when b5.Rehab is null then 0 else b5.Rehab end) + (case when b6.Rehab is null then 0 else b6.Rehab end)
    + (case when b7.Rehab is null then 0 else b7.Rehab end) + (case when b8.Rehab is null then 0 else b8.Rehab end)
    + (case when b9.Rehab is null then 0 else b9.Rehab end) + (case when b10.Rehab is null then 0 else b10.Rehab end)
    + (case when b11.Rehab is null then 0 else b11.Rehab end) + (case when b12.Rehab is null then 0 else b12.Rehab end)
    + (case when b13.Rehab is null then 0 else b13.Rehab end) + (case when b14.Rehab is null then 0 else b14.Rehab end)
    + (case when b15.Rehab is null then 0 else b15.Rehab end) + (case when b16.Rehab is null then 0 else b16.Rehab end)
    + (case when b17.Rehab is null then 0 else b17.Rehab end) + (case when b18.Rehab is null then 0 else b18.Rehab end)
    + (case when b19.Rehab is null then 0 else b19.Rehab end) + (case when b20.Rehab is null then 0 else b20.Rehab end)
    + (case when p1.Rehab is null then 0 else p1.Rehab end) + (case when p2.Rehab is null then 0 else p2.Rehab end)
    + (case when p3.Rehab is null then 0 else p3.Rehab end) + (case when p4.Rehab is null then 0 else p4.Rehab end)
    + (case when p5.Rehab is null then 0 else p5.Rehab end) + (case when p6.Rehab is null then 0 else p6.Rehab end)
    + (case when p7.Rehab is null then 0 else p7.Rehab end) + (case when p8.Rehab is null then 0 else p8.Rehab end)
    + (case when p9.Rehab is null then 0 else p9.Rehab end) + (case when p10.Rehab is null then 0 else p10.Rehab end)
    as Rehab
    , (case when b1.Transplant is null then 0 else b1.Transplant end) + (case when b2.Transplant is null then 0 else b2.Transplant end)
    + (case when b3.Transplant is null then 0 else b3.Transplant end) + (case when b4.Transplant is null then 0 else b4.Transplant end)
    + (case when b5.Transplant is null then 0 else b5.Transplant end) + (case when b6.Transplant is null then 0 else b6.Transplant end)
    + (case when b7.Transplant is null then 0 else b7.Transplant end) + (case when b8.Transplant is null then 0 else b8.Transplant end)
    + (case when b9.Transplant is null then 0 else b9.Transplant end) + (case when b10.Transplant is null then 0 else b10.Transplant end)
    + (case when b11.Transplant is null then 0 else b11.Transplant end) + (case when b12.Transplant is null then 0 else b12.Transplant end)
    + (case when b13.Transplant is null then 0 else b13.Transplant end) + (case when b14.Transplant is null then 0 else b14.Transplant end)
    + (case when b15.Transplant is null then 0 else b15.Transplant end) + (case when b16.Transplant is null then 0 else b16.Transplant end)
    + (case when b17.Transplant is null then 0 else b17.Transplant end) + (case when b18.Transplant is null then 0 else b18.Transplant end)
    + (case when b19.Transplant is null then 0 else b19.Transplant end) + (case when b20.Transplant is null then 0 else b20.Transplant end)
    + (case when p1.Transplant is null then 0 else p1.Transplant end) + (case when p2.Transplant is null then 0 else p2.Transplant end)
    + (case when p3.Transplant is null then 0 else p3.Transplant end) + (case when p4.Transplant is null then 0 else p4.Transplant end)
    + (case when p5.Transplant is null then 0 else p5.Transplant end) + (case when p6.Transplant is null then 0 else p6.Transplant end)
    + (case when p7.Transplant is null then 0 else p7.Transplant end) + (case when p8.Transplant is null then 0 else p8.Transplant end)
    + (case when p9.Transplant is null then 0 else p9.Transplant end) + (case when p10.Transplant is null then 0 else p10.Transplant end)
    as Transplant
from NATHALIE.NJB_UNIQUE_CASES a
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b1 on a.case_dx1=b1.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b2 on a.case_dx2=b2.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b3 on a.case_dx3=b3.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b4 on a.case_dx4=b4.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b5 on a.case_dx5=b5.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b6 on a.case_dx6=b6.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b7 on a.case_dx7=b7.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b8 on a.case_dx8=b8.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b9 on a.case_dx9=b9.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b10 on a.case_dx10=b10.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b11 on a.case_dx11=b11.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b12 on a.case_dx12=b12.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b13 on a.case_dx13=b13.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b14 on a.case_dx14=b14.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b15 on a.case_dx15=b15.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b16 on a.case_dx16=b16.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b17 on a.case_dx17=b17.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b18 on a.case_dx18=b18.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b19 on a.case_dx19=b19.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup b20 on a.case_dx20=b20.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p1 on a.case_pr1=p1.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p2 on a.case_pr2=p2.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p3 on a.case_pr3=p3.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p4 on a.case_pr4=p4.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p5 on a.case_pr5=p5.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p6 on a.case_pr6=p6.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p7 on a.case_pr7=p7.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p8 on a.case_pr8=p8.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p9 on a.case_pr9=p9.code
left join NATHALIE.hedis_readmission_value_sets_2016_dedup p10 on a.case_pr10=p10.code
;

/*
QING work end
*/

/*
COMORBIDITIES TABLE
brought in from flatfile.icd10_groups
tkcom
*/
drop table if exists nathalie.njb_icd10_groups;

create table nathalie.njb_icd10_groups as
select code, comorbid_lace
    , case when comorbid_lace in ('Previous myocardial infarction') then 1 else 0 end as PreviousMyocardialInfarction
    , case when comorbid_lace in ('Cerebrovascular disease') then 1 else 0 end as CerebrovascularDisease
    , case when comorbid_lace in ('Peripheral vascular disease') then 1 else 0 end as PeripheralVascularDisease
    , case when comorbid_lace in ('Diabetes without complications') then 1 else 0 end as DiabetesWithoutComplications
    , case when comorbid_lace in ('Congestive heart failure') then 1 else 0 end as CongestiveHeartFailure
    , case when comorbid_lace in ('Diabetes with end organ damage') then 1 else 0 end as DiabetesWithEndOrganDamage
    , case when comorbid_lace in ('Chronic pulmonary disease') then 1 else 0 end as ChronicPulmonaryDisease
    , case when comorbid_lace in ('Mild liver or renal disease') then 1 else 0 end as MildLiverOrRenalDisease
    , case when comorbid_lace in ('Any tumor (including lymphoma or leukemia)') then 1 else 0 end as AnyTumor
    , case when comorbid_lace in ('Dementia') then 1 else 0 end as Dementia
    , case when comorbid_lace in ('Connective tissue disease') then 1 else 0 end as ConnectiveTissueDisease
    , case when comorbid_lace in ('AIDS') then 1 else 0 end as AIDS
    , case when comorbid_lace in ('Moderate or severe liver or renal disease') then 1 else 0 end as ModerateOrSevereLiverOrRenalDisease
    , case when comorbid_lace in ('Metastatic solid tumor') then 1 else 0 end as MetastaticSolidTumor
from flatfile.icd10_groups
;

/*

*/
--to improve code & assign patients with comorbidity flags more frequently/completly you will need to address
--the following cases: look table has value 36.1. Dx fields have either value 36 or value 36.11. Both should
--match value 36.1 from lookup table but currently the matches fail. 
/*

*/
--Add comobilities indicators
drop table if exists nathalie.njb_unique_cases_3;

create table nathalie.njb_unique_cases_3 as
select a.*
    , (case when b1.PreviousMyocardialInfarction is null then 0 else b1.PreviousMyocardialInfarction end) + (case when b2.PreviousMyocardialInfarction is null then 0 else b2.PreviousMyocardialInfarction end)
    + (case when b3.PreviousMyocardialInfarction is null then 0 else b3.PreviousMyocardialInfarction end) + (case when b4.PreviousMyocardialInfarction is null then 0 else b4.PreviousMyocardialInfarction end)
    + (case when b5.PreviousMyocardialInfarction is null then 0 else b5.PreviousMyocardialInfarction end) + (case when b6.PreviousMyocardialInfarction is null then 0 else b6.PreviousMyocardialInfarction end)
    + (case when b7.PreviousMyocardialInfarction is null then 0 else b7.PreviousMyocardialInfarction end) + (case when b8.PreviousMyocardialInfarction is null then 0 else b8.PreviousMyocardialInfarction end)
    + (case when b9.PreviousMyocardialInfarction is null then 0 else b9.PreviousMyocardialInfarction end) + (case when b10.PreviousMyocardialInfarction is null then 0 else b10.PreviousMyocardialInfarction end)
    + (case when b11.PreviousMyocardialInfarction is null then 0 else b11.PreviousMyocardialInfarction end) + (case when b12.PreviousMyocardialInfarction is null then 0 else b12.PreviousMyocardialInfarction end)
    + (case when b13.PreviousMyocardialInfarction is null then 0 else b13.PreviousMyocardialInfarction end) + (case when b14.PreviousMyocardialInfarction is null then 0 else b14.PreviousMyocardialInfarction end)
    + (case when b15.PreviousMyocardialInfarction is null then 0 else b15.PreviousMyocardialInfarction end) + (case when b16.PreviousMyocardialInfarction is null then 0 else b16.PreviousMyocardialInfarction end)
    + (case when b17.PreviousMyocardialInfarction is null then 0 else b17.PreviousMyocardialInfarction end) + (case when b18.PreviousMyocardialInfarction is null then 0 else b18.PreviousMyocardialInfarction end)
    + (case when b19.PreviousMyocardialInfarction is null then 0 else b19.PreviousMyocardialInfarction end) + (case when b20.PreviousMyocardialInfarction is null then 0 else b20.PreviousMyocardialInfarction end)
    + (case when p1.PreviousMyocardialInfarction is null then 0 else p1.PreviousMyocardialInfarction end) + (case when p2.PreviousMyocardialInfarction is null then 0 else p2.PreviousMyocardialInfarction end)
    + (case when p3.PreviousMyocardialInfarction is null then 0 else p3.PreviousMyocardialInfarction end) + (case when p4.PreviousMyocardialInfarction is null then 0 else p4.PreviousMyocardialInfarction end)
    + (case when p5.PreviousMyocardialInfarction is null then 0 else p5.PreviousMyocardialInfarction end) + (case when p6.PreviousMyocardialInfarction is null then 0 else p6.PreviousMyocardialInfarction end)
    + (case when p7.PreviousMyocardialInfarction is null then 0 else p7.PreviousMyocardialInfarction end) + (case when p8.PreviousMyocardialInfarction is null then 0 else p8.PreviousMyocardialInfarction end)
    + (case when p9.PreviousMyocardialInfarction is null then 0 else p9.PreviousMyocardialInfarction end) + (case when p10.PreviousMyocardialInfarction is null then 0 else p10.PreviousMyocardialInfarction end)
    as PreviousMyocardialInfarction
    , (case when b1.CerebrovascularDisease is null then 0 else b1.CerebrovascularDisease end) + (case when b2.CerebrovascularDisease is null then 0 else b2.CerebrovascularDisease end)
    + (case when b3.CerebrovascularDisease is null then 0 else b3.CerebrovascularDisease end) + (case when b4.CerebrovascularDisease is null then 0 else b4.CerebrovascularDisease end)
    + (case when b5.CerebrovascularDisease is null then 0 else b5.CerebrovascularDisease end) + (case when b6.CerebrovascularDisease is null then 0 else b6.CerebrovascularDisease end)
    + (case when b7.CerebrovascularDisease is null then 0 else b7.CerebrovascularDisease end) + (case when b8.CerebrovascularDisease is null then 0 else b8.CerebrovascularDisease end)
    + (case when b9.CerebrovascularDisease is null then 0 else b9.CerebrovascularDisease end) + (case when b10.CerebrovascularDisease is null then 0 else b10.CerebrovascularDisease end)
    + (case when b11.CerebrovascularDisease is null then 0 else b11.CerebrovascularDisease end) + (case when b12.CerebrovascularDisease is null then 0 else b12.CerebrovascularDisease end)
    + (case when b13.CerebrovascularDisease is null then 0 else b13.CerebrovascularDisease end) + (case when b14.CerebrovascularDisease is null then 0 else b14.CerebrovascularDisease end)
    + (case when b15.CerebrovascularDisease is null then 0 else b15.CerebrovascularDisease end) + (case when b16.CerebrovascularDisease is null then 0 else b16.CerebrovascularDisease end)
    + (case when b17.CerebrovascularDisease is null then 0 else b17.CerebrovascularDisease end) + (case when b18.CerebrovascularDisease is null then 0 else b18.CerebrovascularDisease end)
    + (case when b19.CerebrovascularDisease is null then 0 else b19.CerebrovascularDisease end) + (case when b20.CerebrovascularDisease is null then 0 else b20.CerebrovascularDisease end)
    + (case when p1.CerebrovascularDisease is null then 0 else p1.CerebrovascularDisease end) + (case when p2.CerebrovascularDisease is null then 0 else p2.CerebrovascularDisease end)
    + (case when p3.CerebrovascularDisease is null then 0 else p3.CerebrovascularDisease end) + (case when p4.CerebrovascularDisease is null then 0 else p4.CerebrovascularDisease end)
    + (case when p5.CerebrovascularDisease is null then 0 else p5.CerebrovascularDisease end) + (case when p6.CerebrovascularDisease is null then 0 else p6.CerebrovascularDisease end)
    + (case when p7.CerebrovascularDisease is null then 0 else p7.CerebrovascularDisease end) + (case when p8.CerebrovascularDisease is null then 0 else p8.CerebrovascularDisease end)
    + (case when p9.CerebrovascularDisease is null then 0 else p9.CerebrovascularDisease end) + (case when p10.CerebrovascularDisease is null then 0 else p10.CerebrovascularDisease end)
    as CerebrovascularDisease
    , (case when b1.PeripheralVascularDisease is null then 0 else b1.PeripheralVascularDisease end) + (case when b2.PeripheralVascularDisease is null then 0 else b2.PeripheralVascularDisease end)
    + (case when b3.PeripheralVascularDisease is null then 0 else b3.PeripheralVascularDisease end) + (case when b4.PeripheralVascularDisease is null then 0 else b4.PeripheralVascularDisease end)
    + (case when b5.PeripheralVascularDisease is null then 0 else b5.PeripheralVascularDisease end) + (case when b6.PeripheralVascularDisease is null then 0 else b6.PeripheralVascularDisease end)
    + (case when b7.PeripheralVascularDisease is null then 0 else b7.PeripheralVascularDisease end) + (case when b8.PeripheralVascularDisease is null then 0 else b8.PeripheralVascularDisease end)
    + (case when b9.PeripheralVascularDisease is null then 0 else b9.PeripheralVascularDisease end) + (case when b10.PeripheralVascularDisease is null then 0 else b10.PeripheralVascularDisease end)
    + (case when b11.PeripheralVascularDisease is null then 0 else b11.PeripheralVascularDisease end) + (case when b12.PeripheralVascularDisease is null then 0 else b12.PeripheralVascularDisease end)
    + (case when b13.PeripheralVascularDisease is null then 0 else b13.PeripheralVascularDisease end) + (case when b14.PeripheralVascularDisease is null then 0 else b14.PeripheralVascularDisease end)
    + (case when b15.PeripheralVascularDisease is null then 0 else b15.PeripheralVascularDisease end) + (case when b16.PeripheralVascularDisease is null then 0 else b16.PeripheralVascularDisease end)
    + (case when b17.PeripheralVascularDisease is null then 0 else b17.PeripheralVascularDisease end) + (case when b18.PeripheralVascularDisease is null then 0 else b18.PeripheralVascularDisease end)
    + (case when b19.PeripheralVascularDisease is null then 0 else b19.PeripheralVascularDisease end) + (case when b20.PeripheralVascularDisease is null then 0 else b20.PeripheralVascularDisease end)
    + (case when p1.PeripheralVascularDisease is null then 0 else p1.PeripheralVascularDisease end) + (case when p2.PeripheralVascularDisease is null then 0 else p2.PeripheralVascularDisease end)
    + (case when p3.PeripheralVascularDisease is null then 0 else p3.PeripheralVascularDisease end) + (case when p4.PeripheralVascularDisease is null then 0 else p4.PeripheralVascularDisease end)
    + (case when p5.PeripheralVascularDisease is null then 0 else p5.PeripheralVascularDisease end) + (case when p6.PeripheralVascularDisease is null then 0 else p6.PeripheralVascularDisease end)
    + (case when p7.PeripheralVascularDisease is null then 0 else p7.PeripheralVascularDisease end) + (case when p8.PeripheralVascularDisease is null then 0 else p8.PeripheralVascularDisease end)
    + (case when p9.PeripheralVascularDisease is null then 0 else p9.PeripheralVascularDisease end) + (case when p10.PeripheralVascularDisease is null then 0 else p10.PeripheralVascularDisease end)
    as PeripheralVascularDisease
    , (case when b1.DiabetesWithoutComplications is null then 0 else b1.DiabetesWithoutComplications end) + (case when b2.DiabetesWithoutComplications is null then 0 else b2.DiabetesWithoutComplications end)
    + (case when b3.DiabetesWithoutComplications is null then 0 else b3.DiabetesWithoutComplications end) + (case when b4.DiabetesWithoutComplications is null then 0 else b4.DiabetesWithoutComplications end)
    + (case when b5.DiabetesWithoutComplications is null then 0 else b5.DiabetesWithoutComplications end) + (case when b6.DiabetesWithoutComplications is null then 0 else b6.DiabetesWithoutComplications end)
    + (case when b7.DiabetesWithoutComplications is null then 0 else b7.DiabetesWithoutComplications end) + (case when b8.DiabetesWithoutComplications is null then 0 else b8.DiabetesWithoutComplications end)
    + (case when b9.DiabetesWithoutComplications is null then 0 else b9.DiabetesWithoutComplications end) + (case when b10.DiabetesWithoutComplications is null then 0 else b10.DiabetesWithoutComplications end)
    + (case when b11.DiabetesWithoutComplications is null then 0 else b11.DiabetesWithoutComplications end) + (case when b12.DiabetesWithoutComplications is null then 0 else b12.DiabetesWithoutComplications end)
    + (case when b13.DiabetesWithoutComplications is null then 0 else b13.DiabetesWithoutComplications end) + (case when b14.DiabetesWithoutComplications is null then 0 else b14.DiabetesWithoutComplications end)
    + (case when b15.DiabetesWithoutComplications is null then 0 else b15.DiabetesWithoutComplications end) + (case when b16.DiabetesWithoutComplications is null then 0 else b16.DiabetesWithoutComplications end)
    + (case when b17.DiabetesWithoutComplications is null then 0 else b17.DiabetesWithoutComplications end) + (case when b18.DiabetesWithoutComplications is null then 0 else b18.DiabetesWithoutComplications end)
    + (case when b19.DiabetesWithoutComplications is null then 0 else b19.DiabetesWithoutComplications end) + (case when b20.DiabetesWithoutComplications is null then 0 else b20.DiabetesWithoutComplications end)
    + (case when p1.DiabetesWithoutComplications is null then 0 else p1.DiabetesWithoutComplications end) + (case when p2.DiabetesWithoutComplications is null then 0 else p2.DiabetesWithoutComplications end)
    + (case when p3.DiabetesWithoutComplications is null then 0 else p3.DiabetesWithoutComplications end) + (case when p4.DiabetesWithoutComplications is null then 0 else p4.DiabetesWithoutComplications end)
    + (case when p5.DiabetesWithoutComplications is null then 0 else p5.DiabetesWithoutComplications end) + (case when p6.DiabetesWithoutComplications is null then 0 else p6.DiabetesWithoutComplications end)
    + (case when p7.DiabetesWithoutComplications is null then 0 else p7.DiabetesWithoutComplications end) + (case when p8.DiabetesWithoutComplications is null then 0 else p8.DiabetesWithoutComplications end)
    + (case when p9.DiabetesWithoutComplications is null then 0 else p9.DiabetesWithoutComplications end) + (case when p10.DiabetesWithoutComplications is null then 0 else p10.DiabetesWithoutComplications end)
    as DiabetesWithoutComplications
    , (case when b1.CongestiveHeartFailure is null then 0 else b1.CongestiveHeartFailure end) + (case when b2.CongestiveHeartFailure is null then 0 else b2.CongestiveHeartFailure end)
    + (case when b3.CongestiveHeartFailure is null then 0 else b3.CongestiveHeartFailure end) + (case when b4.CongestiveHeartFailure is null then 0 else b4.CongestiveHeartFailure end)
    + (case when b5.CongestiveHeartFailure is null then 0 else b5.CongestiveHeartFailure end) + (case when b6.CongestiveHeartFailure is null then 0 else b6.CongestiveHeartFailure end)
    + (case when b7.CongestiveHeartFailure is null then 0 else b7.CongestiveHeartFailure end) + (case when b8.CongestiveHeartFailure is null then 0 else b8.CongestiveHeartFailure end)
    + (case when b9.CongestiveHeartFailure is null then 0 else b9.CongestiveHeartFailure end) + (case when b10.CongestiveHeartFailure is null then 0 else b10.CongestiveHeartFailure end)
    + (case when b11.CongestiveHeartFailure is null then 0 else b11.CongestiveHeartFailure end) + (case when b12.CongestiveHeartFailure is null then 0 else b12.CongestiveHeartFailure end)
    + (case when b13.CongestiveHeartFailure is null then 0 else b13.CongestiveHeartFailure end) + (case when b14.CongestiveHeartFailure is null then 0 else b14.CongestiveHeartFailure end)
    + (case when b15.CongestiveHeartFailure is null then 0 else b15.CongestiveHeartFailure end) + (case when b16.CongestiveHeartFailure is null then 0 else b16.CongestiveHeartFailure end)
    + (case when b17.CongestiveHeartFailure is null then 0 else b17.CongestiveHeartFailure end) + (case when b18.CongestiveHeartFailure is null then 0 else b18.CongestiveHeartFailure end)
    + (case when b19.CongestiveHeartFailure is null then 0 else b19.CongestiveHeartFailure end) + (case when b20.CongestiveHeartFailure is null then 0 else b20.CongestiveHeartFailure end)
    + (case when p1.CongestiveHeartFailure is null then 0 else p1.CongestiveHeartFailure end) + (case when p2.CongestiveHeartFailure is null then 0 else p2.CongestiveHeartFailure end)
    + (case when p3.CongestiveHeartFailure is null then 0 else p3.CongestiveHeartFailure end) + (case when p4.CongestiveHeartFailure is null then 0 else p4.CongestiveHeartFailure end)
    + (case when p5.CongestiveHeartFailure is null then 0 else p5.CongestiveHeartFailure end) + (case when p6.CongestiveHeartFailure is null then 0 else p6.CongestiveHeartFailure end)
    + (case when p7.CongestiveHeartFailure is null then 0 else p7.CongestiveHeartFailure end) + (case when p8.CongestiveHeartFailure is null then 0 else p8.CongestiveHeartFailure end)
    + (case when p9.CongestiveHeartFailure is null then 0 else p9.CongestiveHeartFailure end) + (case when p10.CongestiveHeartFailure is null then 0 else p10.CongestiveHeartFailure end)
    as CongestiveHeartFailure
    , (case when b1.DiabetesWithEndOrganDamage is null then 0 else b1.DiabetesWithEndOrganDamage end) + (case when b2.DiabetesWithEndOrganDamage is null then 0 else b2.DiabetesWithEndOrganDamage end)
    + (case when b3.DiabetesWithEndOrganDamage is null then 0 else b3.DiabetesWithEndOrganDamage end) + (case when b4.DiabetesWithEndOrganDamage is null then 0 else b4.DiabetesWithEndOrganDamage end)
    + (case when b5.DiabetesWithEndOrganDamage is null then 0 else b5.DiabetesWithEndOrganDamage end) + (case when b6.DiabetesWithEndOrganDamage is null then 0 else b6.DiabetesWithEndOrganDamage end)
    + (case when b7.DiabetesWithEndOrganDamage is null then 0 else b7.DiabetesWithEndOrganDamage end) + (case when b8.DiabetesWithEndOrganDamage is null then 0 else b8.DiabetesWithEndOrganDamage end)
    + (case when b9.DiabetesWithEndOrganDamage is null then 0 else b9.DiabetesWithEndOrganDamage end) + (case when b10.DiabetesWithEndOrganDamage is null then 0 else b10.DiabetesWithEndOrganDamage end)
    + (case when b11.DiabetesWithEndOrganDamage is null then 0 else b11.DiabetesWithEndOrganDamage end) + (case when b12.DiabetesWithEndOrganDamage is null then 0 else b12.DiabetesWithEndOrganDamage end)
    + (case when b13.DiabetesWithEndOrganDamage is null then 0 else b13.DiabetesWithEndOrganDamage end) + (case when b14.DiabetesWithEndOrganDamage is null then 0 else b14.DiabetesWithEndOrganDamage end)
    + (case when b15.DiabetesWithEndOrganDamage is null then 0 else b15.DiabetesWithEndOrganDamage end) + (case when b16.DiabetesWithEndOrganDamage is null then 0 else b16.DiabetesWithEndOrganDamage end)
    + (case when b17.DiabetesWithEndOrganDamage is null then 0 else b17.DiabetesWithEndOrganDamage end) + (case when b18.DiabetesWithEndOrganDamage is null then 0 else b18.DiabetesWithEndOrganDamage end)
    + (case when b19.DiabetesWithEndOrganDamage is null then 0 else b19.DiabetesWithEndOrganDamage end) + (case when b20.DiabetesWithEndOrganDamage is null then 0 else b20.DiabetesWithEndOrganDamage end)
    + (case when p1.DiabetesWithEndOrganDamage is null then 0 else p1.DiabetesWithEndOrganDamage end) + (case when p2.DiabetesWithEndOrganDamage is null then 0 else p2.DiabetesWithEndOrganDamage end)
    + (case when p3.DiabetesWithEndOrganDamage is null then 0 else p3.DiabetesWithEndOrganDamage end) + (case when p4.DiabetesWithEndOrganDamage is null then 0 else p4.DiabetesWithEndOrganDamage end)
    + (case when p5.DiabetesWithEndOrganDamage is null then 0 else p5.DiabetesWithEndOrganDamage end) + (case when p6.DiabetesWithEndOrganDamage is null then 0 else p6.DiabetesWithEndOrganDamage end)
    + (case when p7.DiabetesWithEndOrganDamage is null then 0 else p7.DiabetesWithEndOrganDamage end) + (case when p8.DiabetesWithEndOrganDamage is null then 0 else p8.DiabetesWithEndOrganDamage end)
    + (case when p9.DiabetesWithEndOrganDamage is null then 0 else p9.DiabetesWithEndOrganDamage end) + (case when p10.DiabetesWithEndOrganDamage is null then 0 else p10.DiabetesWithEndOrganDamage end)
    as DiabetesWithEndOrganDamage
    , (case when b1.ChronicPulmonaryDisease is null then 0 else b1.ChronicPulmonaryDisease end) + (case when b2.ChronicPulmonaryDisease is null then 0 else b2.ChronicPulmonaryDisease end)
    + (case when b3.ChronicPulmonaryDisease is null then 0 else b3.ChronicPulmonaryDisease end) + (case when b4.ChronicPulmonaryDisease is null then 0 else b4.ChronicPulmonaryDisease end)
    + (case when b5.ChronicPulmonaryDisease is null then 0 else b5.ChronicPulmonaryDisease end) + (case when b6.ChronicPulmonaryDisease is null then 0 else b6.ChronicPulmonaryDisease end)
    + (case when b7.ChronicPulmonaryDisease is null then 0 else b7.ChronicPulmonaryDisease end) + (case when b8.ChronicPulmonaryDisease is null then 0 else b8.ChronicPulmonaryDisease end)
    + (case when b9.ChronicPulmonaryDisease is null then 0 else b9.ChronicPulmonaryDisease end) + (case when b10.ChronicPulmonaryDisease is null then 0 else b10.ChronicPulmonaryDisease end)
    + (case when b11.ChronicPulmonaryDisease is null then 0 else b11.ChronicPulmonaryDisease end) + (case when b12.ChronicPulmonaryDisease is null then 0 else b12.ChronicPulmonaryDisease end)
    + (case when b13.ChronicPulmonaryDisease is null then 0 else b13.ChronicPulmonaryDisease end) + (case when b14.ChronicPulmonaryDisease is null then 0 else b14.ChronicPulmonaryDisease end)
    + (case when b15.ChronicPulmonaryDisease is null then 0 else b15.ChronicPulmonaryDisease end) + (case when b16.ChronicPulmonaryDisease is null then 0 else b16.ChronicPulmonaryDisease end)
    + (case when b17.ChronicPulmonaryDisease is null then 0 else b17.ChronicPulmonaryDisease end) + (case when b18.ChronicPulmonaryDisease is null then 0 else b18.ChronicPulmonaryDisease end)
    + (case when b19.ChronicPulmonaryDisease is null then 0 else b19.ChronicPulmonaryDisease end) + (case when b20.ChronicPulmonaryDisease is null then 0 else b20.ChronicPulmonaryDisease end)
    + (case when p1.ChronicPulmonaryDisease is null then 0 else p1.ChronicPulmonaryDisease end) + (case when p2.ChronicPulmonaryDisease is null then 0 else p2.ChronicPulmonaryDisease end)
    + (case when p3.ChronicPulmonaryDisease is null then 0 else p3.ChronicPulmonaryDisease end) + (case when p4.ChronicPulmonaryDisease is null then 0 else p4.ChronicPulmonaryDisease end)
    + (case when p5.ChronicPulmonaryDisease is null then 0 else p5.ChronicPulmonaryDisease end) + (case when p6.ChronicPulmonaryDisease is null then 0 else p6.ChronicPulmonaryDisease end)
    + (case when p7.ChronicPulmonaryDisease is null then 0 else p7.ChronicPulmonaryDisease end) + (case when p8.ChronicPulmonaryDisease is null then 0 else p8.ChronicPulmonaryDisease end)
    + (case when p9.ChronicPulmonaryDisease is null then 0 else p9.ChronicPulmonaryDisease end) + (case when p10.ChronicPulmonaryDisease is null then 0 else p10.ChronicPulmonaryDisease end)
    as ChronicPulmonaryDisease
    , (case when b1.MildLiverOrRenalDisease is null then 0 else b1.MildLiverOrRenalDisease end) + (case when b2.MildLiverOrRenalDisease is null then 0 else b2.MildLiverOrRenalDisease end)
    + (case when b3.MildLiverOrRenalDisease is null then 0 else b3.MildLiverOrRenalDisease end) + (case when b4.MildLiverOrRenalDisease is null then 0 else b4.MildLiverOrRenalDisease end)
    + (case when b5.MildLiverOrRenalDisease is null then 0 else b5.MildLiverOrRenalDisease end) + (case when b6.MildLiverOrRenalDisease is null then 0 else b6.MildLiverOrRenalDisease end)
    + (case when b7.MildLiverOrRenalDisease is null then 0 else b7.MildLiverOrRenalDisease end) + (case when b8.MildLiverOrRenalDisease is null then 0 else b8.MildLiverOrRenalDisease end)
    + (case when b9.MildLiverOrRenalDisease is null then 0 else b9.MildLiverOrRenalDisease end) + (case when b10.MildLiverOrRenalDisease is null then 0 else b10.MildLiverOrRenalDisease end)
    + (case when b11.MildLiverOrRenalDisease is null then 0 else b11.MildLiverOrRenalDisease end) + (case when b12.MildLiverOrRenalDisease is null then 0 else b12.MildLiverOrRenalDisease end)
    + (case when b13.MildLiverOrRenalDisease is null then 0 else b13.MildLiverOrRenalDisease end) + (case when b14.MildLiverOrRenalDisease is null then 0 else b14.MildLiverOrRenalDisease end)
    + (case when b15.MildLiverOrRenalDisease is null then 0 else b15.MildLiverOrRenalDisease end) + (case when b16.MildLiverOrRenalDisease is null then 0 else b16.MildLiverOrRenalDisease end)
    + (case when b17.MildLiverOrRenalDisease is null then 0 else b17.MildLiverOrRenalDisease end) + (case when b18.MildLiverOrRenalDisease is null then 0 else b18.MildLiverOrRenalDisease end)
    + (case when b19.MildLiverOrRenalDisease is null then 0 else b19.MildLiverOrRenalDisease end) + (case when b20.MildLiverOrRenalDisease is null then 0 else b20.MildLiverOrRenalDisease end)
    + (case when p1.MildLiverOrRenalDisease is null then 0 else p1.MildLiverOrRenalDisease end) + (case when p2.MildLiverOrRenalDisease is null then 0 else p2.MildLiverOrRenalDisease end)
    + (case when p3.MildLiverOrRenalDisease is null then 0 else p3.MildLiverOrRenalDisease end) + (case when p4.MildLiverOrRenalDisease is null then 0 else p4.MildLiverOrRenalDisease end)
    + (case when p5.MildLiverOrRenalDisease is null then 0 else p5.MildLiverOrRenalDisease end) + (case when p6.MildLiverOrRenalDisease is null then 0 else p6.MildLiverOrRenalDisease end)
    + (case when p7.MildLiverOrRenalDisease is null then 0 else p7.MildLiverOrRenalDisease end) + (case when p8.MildLiverOrRenalDisease is null then 0 else p8.MildLiverOrRenalDisease end)
    + (case when p9.MildLiverOrRenalDisease is null then 0 else p9.MildLiverOrRenalDisease end) + (case when p10.MildLiverOrRenalDisease is null then 0 else p10.MildLiverOrRenalDisease end)
    as MildLiverOrRenalDisease
    , (case when b1.AnyTumor is null then 0 else b1.AnyTumor end) + (case when b2.AnyTumor is null then 0 else b2.AnyTumor end)
    + (case when b3.AnyTumor is null then 0 else b3.AnyTumor end) + (case when b4.AnyTumor is null then 0 else b4.AnyTumor end)
    + (case when b5.AnyTumor is null then 0 else b5.AnyTumor end) + (case when b6.AnyTumor is null then 0 else b6.AnyTumor end)
    + (case when b7.AnyTumor is null then 0 else b7.AnyTumor end) + (case when b8.AnyTumor is null then 0 else b8.AnyTumor end)
    + (case when b9.AnyTumor is null then 0 else b9.AnyTumor end) + (case when b10.AnyTumor is null then 0 else b10.AnyTumor end)
    + (case when b11.AnyTumor is null then 0 else b11.AnyTumor end) + (case when b12.AnyTumor is null then 0 else b12.AnyTumor end)
    + (case when b13.AnyTumor is null then 0 else b13.AnyTumor end) + (case when b14.AnyTumor is null then 0 else b14.AnyTumor end)
    + (case when b15.AnyTumor is null then 0 else b15.AnyTumor end) + (case when b16.AnyTumor is null then 0 else b16.AnyTumor end)
    + (case when b17.AnyTumor is null then 0 else b17.AnyTumor end) + (case when b18.AnyTumor is null then 0 else b18.AnyTumor end)
    + (case when b19.AnyTumor is null then 0 else b19.AnyTumor end) + (case when b20.AnyTumor is null then 0 else b20.AnyTumor end)
    + (case when p1.AnyTumor is null then 0 else p1.AnyTumor end) + (case when p2.AnyTumor is null then 0 else p2.AnyTumor end)
    + (case when p3.AnyTumor is null then 0 else p3.AnyTumor end) + (case when p4.AnyTumor is null then 0 else p4.AnyTumor end)
    + (case when p5.AnyTumor is null then 0 else p5.AnyTumor end) + (case when p6.AnyTumor is null then 0 else p6.AnyTumor end)
    + (case when p7.AnyTumor is null then 0 else p7.AnyTumor end) + (case when p8.AnyTumor is null then 0 else p8.AnyTumor end)
    + (case when p9.AnyTumor is null then 0 else p9.AnyTumor end) + (case when p10.AnyTumor is null then 0 else p10.AnyTumor end)
    as AnyTumor
    , (case when b1.Dementia is null then 0 else b1.Dementia end) + (case when b2.Dementia is null then 0 else b2.Dementia end)
    + (case when b3.Dementia is null then 0 else b3.Dementia end) + (case when b4.Dementia is null then 0 else b4.Dementia end)
    + (case when b5.Dementia is null then 0 else b5.Dementia end) + (case when b6.Dementia is null then 0 else b6.Dementia end)
    + (case when b7.Dementia is null then 0 else b7.Dementia end) + (case when b8.Dementia is null then 0 else b8.Dementia end)
    + (case when b9.Dementia is null then 0 else b9.Dementia end) + (case when b10.Dementia is null then 0 else b10.Dementia end)
    + (case when b11.Dementia is null then 0 else b11.Dementia end) + (case when b12.Dementia is null then 0 else b12.Dementia end)
    + (case when b13.Dementia is null then 0 else b13.Dementia end) + (case when b14.Dementia is null then 0 else b14.Dementia end)
    + (case when b15.Dementia is null then 0 else b15.Dementia end) + (case when b16.Dementia is null then 0 else b16.Dementia end)
    + (case when b17.Dementia is null then 0 else b17.Dementia end) + (case when b18.Dementia is null then 0 else b18.Dementia end)
    + (case when b19.Dementia is null then 0 else b19.Dementia end) + (case when b20.Dementia is null then 0 else b20.Dementia end)
    + (case when p1.Dementia is null then 0 else p1.Dementia end) + (case when p2.Dementia is null then 0 else p2.Dementia end)
    + (case when p3.Dementia is null then 0 else p3.Dementia end) + (case when p4.Dementia is null then 0 else p4.Dementia end)
    + (case when p5.Dementia is null then 0 else p5.Dementia end) + (case when p6.Dementia is null then 0 else p6.Dementia end)
    + (case when p7.Dementia is null then 0 else p7.Dementia end) + (case when p8.Dementia is null then 0 else p8.Dementia end)
    + (case when p9.Dementia is null then 0 else p9.Dementia end) + (case when p10.Dementia is null then 0 else p10.Dementia end)
    as Dementia
    , (case when b1.ConnectiveTissueDisease is null then 0 else b1.ConnectiveTissueDisease end) + (case when b2.ConnectiveTissueDisease is null then 0 else b2.ConnectiveTissueDisease end)
    + (case when b3.ConnectiveTissueDisease is null then 0 else b3.ConnectiveTissueDisease end) + (case when b4.ConnectiveTissueDisease is null then 0 else b4.ConnectiveTissueDisease end)
    + (case when b5.ConnectiveTissueDisease is null then 0 else b5.ConnectiveTissueDisease end) + (case when b6.ConnectiveTissueDisease is null then 0 else b6.ConnectiveTissueDisease end)
    + (case when b7.ConnectiveTissueDisease is null then 0 else b7.ConnectiveTissueDisease end) + (case when b8.ConnectiveTissueDisease is null then 0 else b8.ConnectiveTissueDisease end)
    + (case when b9.ConnectiveTissueDisease is null then 0 else b9.ConnectiveTissueDisease end) + (case when b10.ConnectiveTissueDisease is null then 0 else b10.ConnectiveTissueDisease end)
    + (case when b11.ConnectiveTissueDisease is null then 0 else b11.ConnectiveTissueDisease end) + (case when b12.ConnectiveTissueDisease is null then 0 else b12.ConnectiveTissueDisease end)
    + (case when b13.ConnectiveTissueDisease is null then 0 else b13.ConnectiveTissueDisease end) + (case when b14.ConnectiveTissueDisease is null then 0 else b14.ConnectiveTissueDisease end)
    + (case when b15.ConnectiveTissueDisease is null then 0 else b15.ConnectiveTissueDisease end) + (case when b16.ConnectiveTissueDisease is null then 0 else b16.ConnectiveTissueDisease end)
    + (case when b17.ConnectiveTissueDisease is null then 0 else b17.ConnectiveTissueDisease end) + (case when b18.ConnectiveTissueDisease is null then 0 else b18.ConnectiveTissueDisease end)
    + (case when b19.ConnectiveTissueDisease is null then 0 else b19.ConnectiveTissueDisease end) + (case when b20.ConnectiveTissueDisease is null then 0 else b20.ConnectiveTissueDisease end)
    + (case when p1.ConnectiveTissueDisease is null then 0 else p1.ConnectiveTissueDisease end) + (case when p2.ConnectiveTissueDisease is null then 0 else p2.ConnectiveTissueDisease end)
    + (case when p3.ConnectiveTissueDisease is null then 0 else p3.ConnectiveTissueDisease end) + (case when p4.ConnectiveTissueDisease is null then 0 else p4.ConnectiveTissueDisease end)
    + (case when p5.ConnectiveTissueDisease is null then 0 else p5.ConnectiveTissueDisease end) + (case when p6.ConnectiveTissueDisease is null then 0 else p6.ConnectiveTissueDisease end)
    + (case when p7.ConnectiveTissueDisease is null then 0 else p7.ConnectiveTissueDisease end) + (case when p8.ConnectiveTissueDisease is null then 0 else p8.ConnectiveTissueDisease end)
    + (case when p9.ConnectiveTissueDisease is null then 0 else p9.ConnectiveTissueDisease end) + (case when p10.ConnectiveTissueDisease is null then 0 else p10.ConnectiveTissueDisease end)
    as ConnectiveTissueDisease
    , (case when b1.AIDS is null then 0 else b1.AIDS end) + (case when b2.AIDS is null then 0 else b2.AIDS end)
    + (case when b3.AIDS is null then 0 else b3.AIDS end) + (case when b4.AIDS is null then 0 else b4.AIDS end)
    + (case when b5.AIDS is null then 0 else b5.AIDS end) + (case when b6.AIDS is null then 0 else b6.AIDS end)
    + (case when b7.AIDS is null then 0 else b7.AIDS end) + (case when b8.AIDS is null then 0 else b8.AIDS end)
    + (case when b9.AIDS is null then 0 else b9.AIDS end) + (case when b10.AIDS is null then 0 else b10.AIDS end)
    + (case when b11.AIDS is null then 0 else b11.AIDS end) + (case when b12.AIDS is null then 0 else b12.AIDS end)
    + (case when b13.AIDS is null then 0 else b13.AIDS end) + (case when b14.AIDS is null then 0 else b14.AIDS end)
    + (case when b15.AIDS is null then 0 else b15.AIDS end) + (case when b16.AIDS is null then 0 else b16.AIDS end)
    + (case when b17.AIDS is null then 0 else b17.AIDS end) + (case when b18.AIDS is null then 0 else b18.AIDS end)
    + (case when b19.AIDS is null then 0 else b19.AIDS end) + (case when b20.AIDS is null then 0 else b20.AIDS end)
    + (case when p1.AIDS is null then 0 else p1.AIDS end) + (case when p2.AIDS is null then 0 else p2.AIDS end)
    + (case when p3.AIDS is null then 0 else p3.AIDS end) + (case when p4.AIDS is null then 0 else p4.AIDS end)
    + (case when p5.AIDS is null then 0 else p5.AIDS end) + (case when p6.AIDS is null then 0 else p6.AIDS end)
    + (case when p7.AIDS is null then 0 else p7.AIDS end) + (case when p8.AIDS is null then 0 else p8.AIDS end)
    + (case when p9.AIDS is null then 0 else p9.AIDS end) + (case when p10.AIDS is null then 0 else p10.AIDS end)
    as AIDS
    , (case when b1.ModerateOrSevereLiverOrRenalDisease is null then 0 else b1.ModerateOrSevereLiverOrRenalDisease end) + (case when b2.ModerateOrSevereLiverOrRenalDisease is null then 0 else b2.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b3.ModerateOrSevereLiverOrRenalDisease is null then 0 else b3.ModerateOrSevereLiverOrRenalDisease end) + (case when b4.ModerateOrSevereLiverOrRenalDisease is null then 0 else b4.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b5.ModerateOrSevereLiverOrRenalDisease is null then 0 else b5.ModerateOrSevereLiverOrRenalDisease end) + (case when b6.ModerateOrSevereLiverOrRenalDisease is null then 0 else b6.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b7.ModerateOrSevereLiverOrRenalDisease is null then 0 else b7.ModerateOrSevereLiverOrRenalDisease end) + (case when b8.ModerateOrSevereLiverOrRenalDisease is null then 0 else b8.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b9.ModerateOrSevereLiverOrRenalDisease is null then 0 else b9.ModerateOrSevereLiverOrRenalDisease end) + (case when b10.ModerateOrSevereLiverOrRenalDisease is null then 0 else b10.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b11.ModerateOrSevereLiverOrRenalDisease is null then 0 else b11.ModerateOrSevereLiverOrRenalDisease end) + (case when b12.ModerateOrSevereLiverOrRenalDisease is null then 0 else b12.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b13.ModerateOrSevereLiverOrRenalDisease is null then 0 else b13.ModerateOrSevereLiverOrRenalDisease end) + (case when b14.ModerateOrSevereLiverOrRenalDisease is null then 0 else b14.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b15.ModerateOrSevereLiverOrRenalDisease is null then 0 else b15.ModerateOrSevereLiverOrRenalDisease end) + (case when b16.ModerateOrSevereLiverOrRenalDisease is null then 0 else b16.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b17.ModerateOrSevereLiverOrRenalDisease is null then 0 else b17.ModerateOrSevereLiverOrRenalDisease end) + (case when b18.ModerateOrSevereLiverOrRenalDisease is null then 0 else b18.ModerateOrSevereLiverOrRenalDisease end)
    + (case when b19.ModerateOrSevereLiverOrRenalDisease is null then 0 else b19.ModerateOrSevereLiverOrRenalDisease end) + (case when b20.ModerateOrSevereLiverOrRenalDisease is null then 0 else b20.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p1.ModerateOrSevereLiverOrRenalDisease is null then 0 else p1.ModerateOrSevereLiverOrRenalDisease end) + (case when p2.ModerateOrSevereLiverOrRenalDisease is null then 0 else p2.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p3.ModerateOrSevereLiverOrRenalDisease is null then 0 else p3.ModerateOrSevereLiverOrRenalDisease end) + (case when p4.ModerateOrSevereLiverOrRenalDisease is null then 0 else p4.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p5.ModerateOrSevereLiverOrRenalDisease is null then 0 else p5.ModerateOrSevereLiverOrRenalDisease end) + (case when p6.ModerateOrSevereLiverOrRenalDisease is null then 0 else p6.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p7.ModerateOrSevereLiverOrRenalDisease is null then 0 else p7.ModerateOrSevereLiverOrRenalDisease end) + (case when p8.ModerateOrSevereLiverOrRenalDisease is null then 0 else p8.ModerateOrSevereLiverOrRenalDisease end)
    + (case when p9.ModerateOrSevereLiverOrRenalDisease is null then 0 else p9.ModerateOrSevereLiverOrRenalDisease end) + (case when p10.ModerateOrSevereLiverOrRenalDisease is null then 0 else p10.ModerateOrSevereLiverOrRenalDisease end)
    as ModerateOrSevereLiverOrRenalDisease
    , (case when b1.MetastaticSolidTumor is null then 0 else b1.MetastaticSolidTumor end) + (case when b2.MetastaticSolidTumor is null then 0 else b2.MetastaticSolidTumor end)
    + (case when b3.MetastaticSolidTumor is null then 0 else b3.MetastaticSolidTumor end) + (case when b4.MetastaticSolidTumor is null then 0 else b4.MetastaticSolidTumor end)
    + (case when b5.MetastaticSolidTumor is null then 0 else b5.MetastaticSolidTumor end) + (case when b6.MetastaticSolidTumor is null then 0 else b6.MetastaticSolidTumor end)
    + (case when b7.MetastaticSolidTumor is null then 0 else b7.MetastaticSolidTumor end) + (case when b8.MetastaticSolidTumor is null then 0 else b8.MetastaticSolidTumor end)
    + (case when b9.MetastaticSolidTumor is null then 0 else b9.MetastaticSolidTumor end) + (case when b10.MetastaticSolidTumor is null then 0 else b10.MetastaticSolidTumor end)
    + (case when b11.MetastaticSolidTumor is null then 0 else b11.MetastaticSolidTumor end) + (case when b12.MetastaticSolidTumor is null then 0 else b12.MetastaticSolidTumor end)
    + (case when b13.MetastaticSolidTumor is null then 0 else b13.MetastaticSolidTumor end) + (case when b14.MetastaticSolidTumor is null then 0 else b14.MetastaticSolidTumor end)
    + (case when b15.MetastaticSolidTumor is null then 0 else b15.MetastaticSolidTumor end) + (case when b16.MetastaticSolidTumor is null then 0 else b16.MetastaticSolidTumor end)
    + (case when b17.MetastaticSolidTumor is null then 0 else b17.MetastaticSolidTumor end) + (case when b18.MetastaticSolidTumor is null then 0 else b18.MetastaticSolidTumor end)
    + (case when b19.MetastaticSolidTumor is null then 0 else b19.MetastaticSolidTumor end) + (case when b20.MetastaticSolidTumor is null then 0 else b20.MetastaticSolidTumor end)
    + (case when p1.MetastaticSolidTumor is null then 0 else p1.MetastaticSolidTumor end) + (case when p2.MetastaticSolidTumor is null then 0 else p2.MetastaticSolidTumor end)
    + (case when p3.MetastaticSolidTumor is null then 0 else p3.MetastaticSolidTumor end) + (case when p4.MetastaticSolidTumor is null then 0 else p4.MetastaticSolidTumor end)
    + (case when p5.MetastaticSolidTumor is null then 0 else p5.MetastaticSolidTumor end) + (case when p6.MetastaticSolidTumor is null then 0 else p6.MetastaticSolidTumor end)
    + (case when p7.MetastaticSolidTumor is null then 0 else p7.MetastaticSolidTumor end) + (case when p8.MetastaticSolidTumor is null then 0 else p8.MetastaticSolidTumor end)
    + (case when p9.MetastaticSolidTumor is null then 0 else p9.MetastaticSolidTumor end) + (case when p10.MetastaticSolidTumor is null then 0 else p10.MetastaticSolidTumor end)
    as MetastaticSolidTumor
from NATHALIE.NJB_UNIQUE_CASES_2 a
left join NATHALIE.njb_icd10_groups b1 on a.case_dx1=b1.code
left join NATHALIE.njb_icd10_groups b2 on a.case_dx2=b2.code
left join NATHALIE.njb_icd10_groups b3 on a.case_dx3=b3.code
left join NATHALIE.njb_icd10_groups b4 on a.case_dx4=b4.code
left join NATHALIE.njb_icd10_groups b5 on a.case_dx5=b5.code
left join NATHALIE.njb_icd10_groups b6 on a.case_dx6=b6.code
left join NATHALIE.njb_icd10_groups b7 on a.case_dx7=b7.code
left join NATHALIE.njb_icd10_groups b8 on a.case_dx8=b8.code
left join NATHALIE.njb_icd10_groups b9 on a.case_dx9=b9.code
left join NATHALIE.njb_icd10_groups b10 on a.case_dx10=b10.code
left join NATHALIE.njb_icd10_groups b11 on a.case_dx11=b11.code
left join NATHALIE.njb_icd10_groups b12 on a.case_dx12=b12.code
left join NATHALIE.njb_icd10_groups b13 on a.case_dx13=b13.code
left join NATHALIE.njb_icd10_groups b14 on a.case_dx14=b14.code
left join NATHALIE.njb_icd10_groups b15 on a.case_dx15=b15.code
left join NATHALIE.njb_icd10_groups b16 on a.case_dx16=b16.code
left join NATHALIE.njb_icd10_groups b17 on a.case_dx17=b17.code
left join NATHALIE.njb_icd10_groups b18 on a.case_dx18=b18.code
left join NATHALIE.njb_icd10_groups b19 on a.case_dx19=b19.code
left join NATHALIE.njb_icd10_groups b20 on a.case_dx20=b20.code
left join NATHALIE.njb_icd10_groups p1 on a.case_pr1=p1.code
left join NATHALIE.njb_icd10_groups p2 on a.case_pr2=p2.code
left join NATHALIE.njb_icd10_groups p3 on a.case_pr3=p3.code
left join NATHALIE.njb_icd10_groups p4 on a.case_pr4=p4.code
left join NATHALIE.njb_icd10_groups p5 on a.case_pr5=p5.code
left join NATHALIE.njb_icd10_groups p6 on a.case_pr6=p6.code
left join NATHALIE.njb_icd10_groups p7 on a.case_pr7=p7.code
left join NATHALIE.njb_icd10_groups p8 on a.case_pr8=p8.code
left join NATHALIE.njb_icd10_groups p9 on a.case_pr9=p9.code
left join NATHALIE.njb_icd10_groups p10 on a.case_pr10=p10.code
;

-- Check results;
select count(*)
        , sum(case when Pregnancy>0 then 1 else 0 end) as Pregnancy
        , sum(case when Chemo>0 then 1 else 0 end) as Chemo
        , sum(case when Rehab>0 then 1 else 0 end) as Rehab
        , sum(case when Transplant>0 then 1 else 0 end) as Transplant
        , sum(case when PreviousMyocardialInfarction>0 then 1 else 0 end) as PreviousMyocardialInfarction
        , sum(case when CerebrovascularDisease>0 then 1 else 0 end) as CerebrovascularDisease
        , sum(case when PeripheralVascularDisease>0 then 1 else 0 end) as PeripheralVascularDisease
        , sum(case when DiabetesWithoutComplications>0 then 1 else 0 end) as DiabetesWithoutComplications
        , sum(case when CongestiveHeartFailure>0 then 1 else 0 end) as CongestiveHeartFailure
        , sum(case when DiabetesWithEndOrganDamage>0 then 1 else 0 end) as DiabetesWithEndOrganDamage
        , sum(case when ChronicPulmonaryDisease>0 then 1 else 0 end) as ChronicPulmonaryDisease
        , sum(case when MildLiverOrRenalDisease>0 then 1 else 0 end) as MildLiverOrRenalDisease
        , sum(case when AnyTumor>0 then 1 else 0 end) as AnyTumor
        , sum(case when Dementia>0 then 1 else 0 end) as Dementia
        , sum(case when ConnectiveTissueDisease>0 then 1 else 0 end) as ConnectiveTissueDisease
        , sum(case when AIDS>0 then 1 else 0 end) as AIDS
        , sum(case when ModerateOrSevereLiverOrRenalDisease>0 then 1 else 0 end) as ModerateOrSevereLiverOrRenalDisease
        , sum(case when MetastaticSolidTumor>0 then 1 else 0 end) as MetastaticSolidTumor
from nathalie.njb_unique_cases_3;
--using member_no: 927051	161943	1768	5489	0	0	0	0	0	0	0	0	0	0	5852	0	0	0	0
--using cin_no: 882221	158836	1679	5147	0	0	0	0	0	0	0	0	0	0	5513	0	0	0	0
--TK comobilities (only Dementia has non-zero values)

/*
count(*)	882221
pregnancy	158836
chemo	1679
rehab	5147
transplant	0
previousmyocardialinfarction	25500
cerebrovasculardisease	11698
peripheralvasculardisease	13385
diabeteswithoutcomplications	57706
congestiveheartfailure	59639
diabeteswithendorgandamage	0
chronicpulmonarydisease	54853
mildliverorrenaldisease	14913
anytumor	12173
dementia	5513
connectivetissuedisease	3996
aids	0
moderateorsevereliverorrenaldisease	77253
metastaticsolidtumor	7044

*/

/*
ABSORB_TRANSFERS ---> no loop
*/

/*
ON THE SUBJECT OF LOOPS: 
Impala does not support any loop syntax (see https://stackoverflow.com/questions/49523380/write-a-while-loop-in-impala-sql)
Therefore as long as you need to fuse admits, you need to manually repeat the search-and-fuse script below. 
*/

/*
The following is alsways the 1st step and is only run one time
*/

drop table if exists nathalie.njb_complete_cases_1;

create table nathalie.njb_complete_cases_1
as
select *
from
(    
    select 
        case_id
        , adm_dt
        , dis_dt
        , cin_no
        , member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , pregnancy, chemo, rehab, transplant
        , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
        , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
        , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
        , severity
        , aprdrg
        , dis_status
        , provider
        , cur_pcp
        , cur_site_no
        , product_code
        , segment
        , yearmth
        , from_er
        , source_table
        , stay_interval
        , paid_amt_case
        , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber --order newly engineered cases above cases that are transfers and whose admit date is later
    from
    ( -- DIS_DT_UPDATED // use stay_interval to compute new discharge date
        select 
            case 
                when stay_interval < 2 then ss_case_id
                else fs_case_id
            end as case_id
            , adm_dt
            ,   case
                    when stay_interval < 2 then ss_dis_dt
                    else fs_dis_dt
                end as dis_dt
            , cin_no
            , member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity
            , aprdrg
            , dis_status
            , provider
            , cur_pcp
            , cur_site_no
            , product_code
            , segment
            , yearmth
            , from_er
            , case
                when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                else fs_source_table
            end as source_table
            , stay_interval
            , case
                when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case
                else fs_paid_amt_case
            end as paid_amt_case
            , case 
                    when (stay_interval < 2 and (fs_pregnancy + ss_pregnancy) > 0) then 1
                    else (case when fs_pregnancy > 1 then 1 else fs_pregnancy end)
                end as pregnancy
                --LOGIC: include pregnancy whether it is flagged during the first stay ('fs') or the second stay ('ss')
            , (case when chemo > 1 then 1 else chemo end) as chemo
            , (case when rehab > 1 then 1 else rehab end) as rehab
            , (case when transplant > 1 then 1 else transplant end) as transplant
            , (case when (stay_interval < 2 and (fs_PreviousMyocardialInfarction + ss_PreviousMyocardialInfarction) > 0) then 1 
                    else (case when fs_PreviousMyocardialInfarction > 1 then 1 else fs_PreviousMyocardialInfarction end) 
                    end) as PreviousMyocardialInfarction
            , (case when (stay_interval < 2 and (fs_CerebrovascularDisease + ss_CerebrovascularDisease) > 0) then 1 
                    else (case when fs_CerebrovascularDisease > 1 then 1 else fs_CerebrovascularDisease end)  
                    end) as CerebrovascularDisease
            , (case when (stay_interval < 2 and (fs_PeripheralVascularDisease + ss_PeripheralVascularDisease) > 0) then 1 
                    else (case when fs_PeripheralVascularDisease > 1 then 1 else fs_PeripheralVascularDisease end)  
                    end) as PeripheralVascularDisease
            , (case when (stay_interval < 2 and (fs_DiabetesWithoutComplications + ss_DiabetesWithoutComplications) > 0) then 1 
                    else (case when fs_DiabetesWithoutComplications > 1 then 1 else fs_DiabetesWithoutComplications end)  
                    end) as DiabetesWithoutComplications
            , (case when (stay_interval < 2 and (fs_CongestiveHeartFailure + ss_CongestiveHeartFailure) > 0) then 1 
                    else (case when fs_CongestiveHeartFailure > 1 then 1 else fs_CongestiveHeartFailure end)  
                    end) as CongestiveHeartFailure
            , (case when (stay_interval < 2 and (fs_DiabetesWithEndOrganDamage + ss_DiabetesWithEndOrganDamage) > 0) then 1 
                    else (case when fs_DiabetesWithEndOrganDamage > 1 then 1 else fs_DiabetesWithEndOrganDamage end)  
                    end) as DiabetesWithEndOrganDamage
            , (case when (stay_interval < 2 and (fs_ChronicPulmonaryDisease + ss_ChronicPulmonaryDisease) > 0) then 1 
                    else (case when fs_ChronicPulmonaryDisease > 1 then 1 else fs_ChronicPulmonaryDisease end)  
                    end) as ChronicPulmonaryDisease
            , (case when (stay_interval < 2 and (fs_MildLiverOrRenalDisease + ss_MildLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_MildLiverOrRenalDisease > 1 then 1 else fs_MildLiverOrRenalDisease end)  
                    end) as MildLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_AnyTumor + ss_AnyTumor) > 0) then 1 
                    else (case when fs_AnyTumor > 1 then 1 else fs_AnyTumor end)  
                    end) as AnyTumor
            , (case when (stay_interval < 2 and (fs_Dementia + ss_Dementia) > 0) then 1 
                    else (case when fs_Dementia > 1 then 1 else fs_Dementia end)  
                    end) as Dementia
            , (case when (stay_interval < 2 and (fs_ConnectiveTissueDisease + ss_ConnectiveTissueDisease) > 0) then 1 
                    else (case when fs_ConnectiveTissueDisease > 1 then 1 else fs_ConnectiveTissueDisease end)  
                    end) as ConnectiveTissueDisease
            , (case when (stay_interval < 2 and (fs_AIDS + ss_AIDS) > 0) then 1 
                    else (case when fs_AIDS > 1 then 1 else fs_AIDS end)  
                    end) as AIDS
            , (case when (stay_interval < 2 and (fs_ModerateOrSevereLiverOrRenalDisease + ss_ModerateOrSevereLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_ModerateOrSevereLiverOrRenalDisease > 1 then 1 else fs_ModerateOrSevereLiverOrRenalDisease end)  
                    end) as ModerateOrSevereLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_MetastaticSolidTumor + ss_MetastaticSolidTumor) > 0) then 1 
                    else (case when fs_MetastaticSolidTumor > 1 then 1 else fs_MetastaticSolidTumor end)  
                    end) as MetastaticSolidTumor
        from
        ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
            select --select with priority QNXT > CLM > ENC and join with MEMMO data
                FS.case_id as fs_case_id, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.cin_no, FS.member_no
                , FS.case_dx1, FS.case_dx2, FS.case_dx3, FS.case_dx4, FS.case_dx5, FS.case_dx6, FS.case_dx7, FS.case_dx8, FS.case_dx9, FS.case_dx10
                , FS.case_dx11, FS.case_dx12, FS.case_dx13, FS.case_dx14, FS.case_dx15, FS.case_dx16, FS.case_dx17, FS.case_dx18, FS.case_dx19, FS.case_dx20 
                , FS.case_pr1, FS.case_pr2, FS.case_pr3, FS.case_pr4, FS.case_pr5, FS.case_pr6, FS.case_pr7, FS.case_pr8, FS.case_pr9, FS.case_pr10
                , FS.severity, FS.aprdrg, FS.dis_status, FS.provider
                , FS.cur_pcp, FS.cur_site_no, FS.product_code, FS.segment, FS.yearmth
                , FS.from_er
                --, RIGHT('00' + CONVERT(VARCHAR, FS.source_table), 2) as fs_source_table
                , cast(FS.source_table as varchar(1)) as fs_source_table
                , SS.dis_dt as ss_dis_dt
                , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                , case
                    --when FS.member_no = SS.member_no 
                    when FS.cin_no = SS.cin_no 
                        then abs(datediff(SS.adm_dt, FS.dis_dt)) -- absolute value selected bc w/ synthetic data, datediff under 1 month were neg, over 1 month pos --> did not find cause but this step appeared to be necessary for correct output
                        else null
                    end as stay_interval
                , FS.paid_amt_case as fs_paid_amt_case
                , SS.paid_amt_case as ss_paid_amt_case
                , FS.pregnancy as fs_pregnancy
                , SS.pregnancy as ss_pregnancy
                --below, matches HEDIS requirement to only flag the 1st stay for chemo, rehab and transplant
                , FS.chemo
                , FS.rehab
                , FS.transplant
                , FS.PreviousMyocardialInfarction as fs_PreviousMyocardialInfarction
                , SS.PreviousMyocardialInfarction as ss_PreviousMyocardialInfarction
                , FS.CerebrovascularDisease as fs_CerebrovascularDisease
                , SS.CerebrovascularDisease as ss_CerebrovascularDisease
                , FS.PeripheralVascularDisease as fs_PeripheralVascularDisease
                , SS.PeripheralVascularDisease as ss_PeripheralVascularDisease
                , FS.DiabetesWithoutComplications as fs_DiabetesWithoutComplications
                , SS.DiabetesWithoutComplications as ss_DiabetesWithoutComplications
                , FS.CongestiveHeartFailure as fs_CongestiveHeartFailure
                , SS.CongestiveHeartFailure as ss_CongestiveHeartFailure
                , FS.DiabetesWithEndOrganDamage as fs_DiabetesWithEndOrganDamage
                , SS.DiabetesWithEndOrganDamage as ss_DiabetesWithEndOrganDamage
                , FS.ChronicPulmonaryDisease as fs_ChronicPulmonaryDisease
                , SS.ChronicPulmonaryDisease as ss_ChronicPulmonaryDisease
                , FS.MildLiverOrRenalDisease as fs_MildLiverOrRenalDisease
                , SS.MildLiverOrRenalDisease as ss_MildLiverOrRenalDisease
                , FS.AnyTumor as fs_AnyTumor
                , SS.AnyTumor as ss_AnyTumor
                , FS.Dementia as fs_Dementia
                , SS.Dementia as ss_Dementia
                , FS.ConnectiveTissueDisease as fs_ConnectiveTissueDisease
                , SS.ConnectiveTissueDisease as ss_ConnectiveTissueDisease
                , FS.AIDS as fs_AIDS
                , SS.AIDS as ss_AIDS
                , FS.ModerateOrSevereLiverOrRenalDisease as fs_ModerateOrSevereLiverOrRenalDisease
                , SS.ModerateOrSevereLiverOrRenalDisease as ss_ModerateOrSevereLiverOrRenalDisease
                , FS.MetastaticSolidTumor as fs_MetastaticSolidTumor
                , SS.MetastaticSolidTumor as ss_MetastaticSolidTumor
            from
            NATHALIE.NJB_UNIQUE_CASES_3 as FS --TK NEED TO SWITCH OUT OF SYNTHETIC DATA FOR ACTUAL QUERY
            --NATHALIE.njb_synthetic_UNIQUE_CASES as FS
            --order by UNIQUE_CASES.rownumber
            inner join
            NATHALIE.NJB_UNIQUE_CASES_3 as SS --TK NEED TO SWITCH OUT OF SYNTHETIC DATA FOR ACTUAL QUERY
            --NATHALIE.njb_synthetic_UNIQUE_CASES as SS
            --order by UNIQUE_CASES.rownumber
            ON SS.rownumber = FS.rownumber + 1
        ) AS INTERVAL_ADDED
    ) AS DIS_DT_UPDATED
) AS ROWNUMER_ADDED
where rownumber = 1
;
--777889

-- Check results;
select count(*)
        , sum(Pregnancy) as Pregnancy
        , sum(Chemo) as Chemo
        , sum(Rehab) as Rehab
        , sum(Transplant) as Transplant
        , sum(PreviousMyocardialInfarction) as PreviousMyocardialInfarction
        , sum(CerebrovascularDisease) as CerebrovascularDisease
        , sum(PeripheralVascularDisease) as PeripheralVascularDisease
        , sum(DiabetesWithoutComplications) as DiabetesWithoutComplications
        , sum(CongestiveHeartFailure) as CongestiveHeartFailure
        , sum(DiabetesWithEndOrganDamage) as DiabetesWithEndOrganDamage
        , sum(ChronicPulmonaryDisease) as ChronicPulmonaryDisease
        , sum(MildLiverOrRenalDisease) as MildLiverOrRenalDisease
        , sum(AnyTumor) as AnyTumor
        , sum(Dementia) as Dementia
        , sum(ConnectiveTissueDisease) as ConnectiveTissueDisease
        , sum(AIDS) as AIDS
        , sum(ModerateOrSevereLiverOrRenalDisease) as ModerateOrSevereLiverOrRenalDisease
        , sum(MetastaticSolidTumor) as MetastaticSolidTumor
--select sum(Dementia)
from nathalie.njb_complete_cases_1;
--777889	141301	1489	4072	0	0	0	0	0	0	0	0	0	0	4839	0	0	0	0

/* 
add rownumber to the final product -- may not be necessary after you've written the loop 
*/
drop table if exists NATHALIE.njb_complete_cases_2;

create table NATHALIE.njb_complete_cases_2 as
select 
    case_id
    , adm_dt
    , dis_dt
    , cin_no
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , pregnancy, chemo, rehab, transplant
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , paid_amt_case
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber
from NATHALIE.njb_complete_cases_1
;


/*
MANUALLY CONTROLLED LOOP
*/

/*
Start with table name: njb_complete_cases_2
Has columns: 
    case_id
    , adm_dt
    , dis_dt
    , cin_no
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , source_table
    , stay_interval
    , paid_amt_case
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber

Transformation: join with offset = 1, look for transfers, incorporate transfer dates and costs to 1 admission, delete transfer and renumber rows. 

End with table name njb_complete_case; has same columns as above
*/
drop table if exists nathalie.njb_complete_cases_3;

create table nathalie.njb_complete_cases_3 
as
select *
from
(    
    select 
        case_id
        , adm_dt
        , dis_dt
        , cin_no
        , member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , pregnancy, chemo, rehab, transplant
        , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
        , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
        , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
        , severity
        , aprdrg
        , dis_status
        , provider
        , cur_pcp
        , cur_site_no
        , product_code
        , segment
        , yearmth
        , from_er
        , source_table
        , stay_interval
        , paid_amt_case
        , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber --order newly engineered cases above cases that are transfers and whose admit date is later
    from
    ( -- DIS_DT_UPDATED // use stay_interval to compute new discharge date
        select 
            case 
                when stay_interval < 2 then ss_case_id
                else fs_case_id
            end as case_id
            , adm_dt
            ,   case
                    when stay_interval < 2 then ss_dis_dt
                    else fs_dis_dt
                end as dis_dt
            , cin_no
            , member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity
            , aprdrg
            , dis_status
            , provider
            , cur_pcp
            , cur_site_no
            , product_code
            , segment
            , yearmth
            , from_er
            , case
                when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                else fs_source_table
            end as source_table
            , stay_interval
            , case
                when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case 
                else fs_paid_amt_case
            end as paid_amt_case
            , case 
                    when (stay_interval < 2 and (fs_pregnancy + ss_pregnancy) > 0) then 1
                    else (case when fs_pregnancy > 1 then 1 else fs_pregnancy end)
                end as pregnancy
                --LOGIC: include pregnancy whether it is flagged duringthe first stay ('fs') or the second stay ('ss')
            , (case when chemo > 1 then 1 else chemo end) as chemo
            , (case when rehab > 1 then 1 else rehab end) as rehab
            , (case when transplant > 1 then 1 else transplant end) as transplant
            , (case when (stay_interval < 2 and (fs_PreviousMyocardialInfarction + ss_PreviousMyocardialInfarction) > 0) then 1 
                    else (case when fs_PreviousMyocardialInfarction > 1 then 1 else fs_PreviousMyocardialInfarction end) 
                    end) as PreviousMyocardialInfarction
            , (case when (stay_interval < 2 and (fs_CerebrovascularDisease + ss_CerebrovascularDisease) > 0) then 1 
                    else (case when fs_CerebrovascularDisease > 1 then 1 else fs_CerebrovascularDisease end)  
                    end) as CerebrovascularDisease
            , (case when (stay_interval < 2 and (fs_PeripheralVascularDisease + ss_PeripheralVascularDisease) > 0) then 1 
                    else (case when fs_PeripheralVascularDisease > 1 then 1 else fs_PeripheralVascularDisease end)  
                    end) as PeripheralVascularDisease
            , (case when (stay_interval < 2 and (fs_DiabetesWithoutComplications + ss_DiabetesWithoutComplications) > 0) then 1 
                    else (case when fs_DiabetesWithoutComplications > 1 then 1 else fs_DiabetesWithoutComplications end)  
                    end) as DiabetesWithoutComplications
            , (case when (stay_interval < 2 and (fs_CongestiveHeartFailure + ss_CongestiveHeartFailure) > 0) then 1 
                    else (case when fs_CongestiveHeartFailure > 1 then 1 else fs_CongestiveHeartFailure end)  
                    end) as CongestiveHeartFailure
            , (case when (stay_interval < 2 and (fs_DiabetesWithEndOrganDamage + ss_DiabetesWithEndOrganDamage) > 0) then 1 
                    else (case when fs_DiabetesWithEndOrganDamage > 1 then 1 else fs_DiabetesWithEndOrganDamage end)  
                    end) as DiabetesWithEndOrganDamage
            , (case when (stay_interval < 2 and (fs_ChronicPulmonaryDisease + ss_ChronicPulmonaryDisease) > 0) then 1 
                    else (case when fs_ChronicPulmonaryDisease > 1 then 1 else fs_ChronicPulmonaryDisease end)  
                    end) as ChronicPulmonaryDisease
            , (case when (stay_interval < 2 and (fs_MildLiverOrRenalDisease + ss_MildLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_MildLiverOrRenalDisease > 1 then 1 else fs_MildLiverOrRenalDisease end)  
                    end) as MildLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_AnyTumor + ss_AnyTumor) > 0) then 1 
                    else (case when fs_AnyTumor > 1 then 1 else fs_AnyTumor end)  
                    end) as AnyTumor
            , (case when (stay_interval < 2 and (fs_Dementia + ss_Dementia) > 0) then 1 
                    else (case when fs_Dementia > 1 then 1 else fs_Dementia end)  
                    end) as Dementia
            , (case when (stay_interval < 2 and (fs_ConnectiveTissueDisease + ss_ConnectiveTissueDisease) > 0) then 1 
                    else (case when fs_ConnectiveTissueDisease > 1 then 1 else fs_ConnectiveTissueDisease end)  
                    end) as ConnectiveTissueDisease
            , (case when (stay_interval < 2 and (fs_AIDS + ss_AIDS) > 0) then 1 
                    else (case when fs_AIDS > 1 then 1 else fs_AIDS end)  
                    end) as AIDS
            , (case when (stay_interval < 2 and (fs_ModerateOrSevereLiverOrRenalDisease + ss_ModerateOrSevereLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_ModerateOrSevereLiverOrRenalDisease > 1 then 1 else fs_ModerateOrSevereLiverOrRenalDisease end)  
                    end) as ModerateOrSevereLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_MetastaticSolidTumor + ss_MetastaticSolidTumor) > 0) then 1 
                    else (case when fs_MetastaticSolidTumor > 1 then 1 else fs_MetastaticSolidTumor end)  
                    end) as MetastaticSolidTumor
        from
        ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
            select --select with priority QNXT > CLM > ENC and join with MEMMO data
                FS.case_id as fs_case_id, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.cin_no, FS.member_no
                , FS.case_dx1, FS.case_dx2, FS.case_dx3, FS.case_dx4, FS.case_dx5, FS.case_dx6, FS.case_dx7, FS.case_dx8, FS.case_dx9, FS.case_dx10
                , FS.case_dx11, FS.case_dx12, FS.case_dx13, FS.case_dx14, FS.case_dx15, FS.case_dx16, FS.case_dx17, FS.case_dx18, FS.case_dx19, FS.case_dx20 
                , FS.case_pr1, FS.case_pr2, FS.case_pr3, FS.case_pr4, FS.case_pr5, FS.case_pr6, FS.case_pr7, FS.case_pr8, FS.case_pr9, FS.case_pr10
                , FS.pregnancy as fs_pregnancy, SS.pregnancy as ss_pregnancy, FS.chemo, FS.rehab, FS.transplant
                , FS.PreviousMyocardialInfarction as fs_PreviousMyocardialInfarction
                , SS.PreviousMyocardialInfarction as ss_PreviousMyocardialInfarction
                , FS.CerebrovascularDisease as fs_CerebrovascularDisease
                , SS.CerebrovascularDisease as ss_CerebrovascularDisease
                , FS.PeripheralVascularDisease as fs_PeripheralVascularDisease
                , SS.PeripheralVascularDisease as ss_PeripheralVascularDisease
                , FS.DiabetesWithoutComplications as fs_DiabetesWithoutComplications
                , SS.DiabetesWithoutComplications as ss_DiabetesWithoutComplications
                , FS.CongestiveHeartFailure as fs_CongestiveHeartFailure
                , SS.CongestiveHeartFailure as ss_CongestiveHeartFailure
                , FS.DiabetesWithEndOrganDamage as fs_DiabetesWithEndOrganDamage
                , SS.DiabetesWithEndOrganDamage as ss_DiabetesWithEndOrganDamage
                , FS.ChronicPulmonaryDisease as fs_ChronicPulmonaryDisease
                , SS.ChronicPulmonaryDisease as ss_ChronicPulmonaryDisease
                , FS.MildLiverOrRenalDisease as fs_MildLiverOrRenalDisease
                , SS.MildLiverOrRenalDisease as ss_MildLiverOrRenalDisease
                , FS.AnyTumor as fs_AnyTumor
                , SS.AnyTumor as ss_AnyTumor
                , FS.Dementia as fs_Dementia
                , SS.Dementia as ss_Dementia
                , FS.ConnectiveTissueDisease as fs_ConnectiveTissueDisease
                , SS.ConnectiveTissueDisease as ss_ConnectiveTissueDisease
                , FS.AIDS as fs_AIDS
                , SS.AIDS as ss_AIDS
                , FS.ModerateOrSevereLiverOrRenalDisease as fs_ModerateOrSevereLiverOrRenalDisease
                , SS.ModerateOrSevereLiverOrRenalDisease as ss_ModerateOrSevereLiverOrRenalDisease
                , FS.MetastaticSolidTumor as fs_MetastaticSolidTumor
                , SS.MetastaticSolidTumor as ss_MetastaticSolidTumor
                , FS.severity, FS.aprdrg, FS.dis_status, FS.provider
                , FS.cur_pcp, FS.cur_site_no, FS.product_code, FS.segment, FS.yearmth
                , FS.from_er
                , cast(FS.source_table as varchar(1)) as fs_source_table
                , SS.dis_dt as ss_dis_dt
                , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                , case
                    when FS.cin_no = SS.cin_no 
                        then abs(datediff(SS.adm_dt, FS.dis_dt))
                        else null
                    end as stay_interval
                , FS.paid_amt_case as fs_paid_amt_case
                , SS.paid_amt_case as ss_paid_amt_case
            from
            NATHALIE.NJB_COMPLETE_CASES_2 as FS 
            inner join
            NATHALIE.NJB_COMPLETE_CASES_2 as SS
            ON SS.rownumber = FS.rownumber + 1
        ) AS INTERVAL_ADDED
    ) AS DIS_DT_UPDATED
) AS ROWNUMER_ADDED
where rownumber = 1
;

-- Check results;
select count(*)
        , sum(Pregnancy) as Pregnancy
        , sum(Chemo) as Chemo
        , sum(Rehab) as Rehab
        , sum(Transplant) as Transplant
        , sum(PreviousMyocardialInfarction) as PreviousMyocardialInfarction
        , sum(CerebrovascularDisease) as CerebrovascularDisease
        , sum(PeripheralVascularDisease) as PeripheralVascularDisease
        , sum(DiabetesWithoutComplications) as DiabetesWithoutComplications
        , sum(CongestiveHeartFailure) as CongestiveHeartFailure
        , sum(DiabetesWithEndOrganDamage) as DiabetesWithEndOrganDamage
        , sum(ChronicPulmonaryDisease) as ChronicPulmonaryDisease
        , sum(MildLiverOrRenalDisease) as MildLiverOrRenalDisease
        , sum(AnyTumor) as AnyTumor
        , sum(Dementia) as Dementia
        , sum(ConnectiveTissueDisease) as ConnectiveTissueDisease
        , sum(AIDS) as AIDS
        , sum(ModerateOrSevereLiverOrRenalDisease) as ModerateOrSevereLiverOrRenalDisease
        , sum(MetastaticSolidTumor) as MetastaticSolidTumor
from nathalie.njb_complete_cases_3;
--776113	141107	1488	4046	0	0	0	0	0	0	0	0	0	0	4834	0	0	0	0


/* 
add rownumber to the final product 
*/
drop table if exists NATHALIE.njb_complete_cases_4;

create table NATHALIE.njb_complete_cases_4 as
select 
    case_id
    , adm_dt
    , dis_dt
    , cin_no
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , pregnancy, chemo, rehab, transplant
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , paid_amt_case
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber
from NATHALIE.njb_complete_cases_3
;

drop table if exists nathalie.njb_complete_cases_5;

create table nathalie.njb_complete_cases_7
as
select *
from
(    
    select 
        case_id
        , adm_dt
        , dis_dt
        , cin_no
        , member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , pregnancy, chemo, rehab, transplant
        , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
        , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
        , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
        , severity
        , aprdrg
        , dis_status
        , provider
        , cur_pcp
        , cur_site_no
        , product_code
        , segment
        , yearmth
        , from_er
        , source_table
        , stay_interval
        , paid_amt_case
        , row_number() over(partition by cin_no, dis_dt order by adm_dt asc) as rownumber --order newly engineered cases above cases that are transfers and whose admit date is later
    from
    ( -- DIS_DT_UPDATED // use stay_interval to compute new discharge date
        select 
            case 
                when stay_interval < 2 then ss_case_id
                else fs_case_id
            end as case_id
            , adm_dt
            ,   case
                    when stay_interval < 2 then ss_dis_dt
                    else fs_dis_dt
                end as dis_dt
            , cin_no
            , member_no
            , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
            , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
            , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
            , severity
            , aprdrg
            , dis_status
            , provider
            , cur_pcp
            , cur_site_no
            , product_code
            , segment
            , yearmth
            , from_er
            , case
                when stay_interval < 2 then concat(fs_source_table, ', ', ss_source_table)
                else fs_source_table
            end as source_table
            , stay_interval
            , case
                when (stay_interval < 2 and fs_case_id != ss_case_id) then fs_paid_amt_case + ss_paid_amt_case 
                else fs_paid_amt_case
            end as paid_amt_case
            , case 
                    when (stay_interval < 2 and (fs_pregnancy + ss_pregnancy) > 0) then 1
                    else (case when fs_pregnancy > 1 then 1 else fs_pregnancy end)
                end as pregnancy
                --LOGIC: include pregnancy whether it is flagged duringthe first stay ('fs') or the second stay ('ss')
            , (case when chemo > 1 then 1 else chemo end) as chemo
            , (case when rehab > 1 then 1 else rehab end) as rehab
            , (case when transplant > 1 then 1 else transplant end) as transplant
            , (case when (stay_interval < 2 and (fs_PreviousMyocardialInfarction + ss_PreviousMyocardialInfarction) > 0) then 1 
                    else (case when fs_PreviousMyocardialInfarction > 1 then 1 else fs_PreviousMyocardialInfarction end) 
                    end) as PreviousMyocardialInfarction
            , (case when (stay_interval < 2 and (fs_CerebrovascularDisease + ss_CerebrovascularDisease) > 0) then 1 
                    else (case when fs_CerebrovascularDisease > 1 then 1 else fs_CerebrovascularDisease end)  
                    end) as CerebrovascularDisease
            , (case when (stay_interval < 2 and (fs_PeripheralVascularDisease + ss_PeripheralVascularDisease) > 0) then 1 
                    else (case when fs_PeripheralVascularDisease > 1 then 1 else fs_PeripheralVascularDisease end)  
                    end) as PeripheralVascularDisease
            , (case when (stay_interval < 2 and (fs_DiabetesWithoutComplications + ss_DiabetesWithoutComplications) > 0) then 1 
                    else (case when fs_DiabetesWithoutComplications > 1 then 1 else fs_DiabetesWithoutComplications end)  
                    end) as DiabetesWithoutComplications
            , (case when (stay_interval < 2 and (fs_CongestiveHeartFailure + ss_CongestiveHeartFailure) > 0) then 1 
                    else (case when fs_CongestiveHeartFailure > 1 then 1 else fs_CongestiveHeartFailure end)  
                    end) as CongestiveHeartFailure
            , (case when (stay_interval < 2 and (fs_DiabetesWithEndOrganDamage + ss_DiabetesWithEndOrganDamage) > 0) then 1 
                    else (case when fs_DiabetesWithEndOrganDamage > 1 then 1 else fs_DiabetesWithEndOrganDamage end)  
                    end) as DiabetesWithEndOrganDamage
            , (case when (stay_interval < 2 and (fs_ChronicPulmonaryDisease + ss_ChronicPulmonaryDisease) > 0) then 1 
                    else (case when fs_ChronicPulmonaryDisease > 1 then 1 else fs_ChronicPulmonaryDisease end)  
                    end) as ChronicPulmonaryDisease
            , (case when (stay_interval < 2 and (fs_MildLiverOrRenalDisease + ss_MildLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_MildLiverOrRenalDisease > 1 then 1 else fs_MildLiverOrRenalDisease end)  
                    end) as MildLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_AnyTumor + ss_AnyTumor) > 0) then 1 
                    else (case when fs_AnyTumor > 1 then 1 else fs_AnyTumor end)  
                    end) as AnyTumor
            , (case when (stay_interval < 2 and (fs_Dementia + ss_Dementia) > 0) then 1 
                    else (case when fs_Dementia > 1 then 1 else fs_Dementia end)  
                    end) as Dementia
            , (case when (stay_interval < 2 and (fs_ConnectiveTissueDisease + ss_ConnectiveTissueDisease) > 0) then 1 
                    else (case when fs_ConnectiveTissueDisease > 1 then 1 else fs_ConnectiveTissueDisease end)  
                    end) as ConnectiveTissueDisease
            , (case when (stay_interval < 2 and (fs_AIDS + ss_AIDS) > 0) then 1 
                    else (case when fs_AIDS > 1 then 1 else fs_AIDS end)  
                    end) as AIDS
            , (case when (stay_interval < 2 and (fs_ModerateOrSevereLiverOrRenalDisease + ss_ModerateOrSevereLiverOrRenalDisease) > 0) then 1 
                    else (case when fs_ModerateOrSevereLiverOrRenalDisease > 1 then 1 else fs_ModerateOrSevereLiverOrRenalDisease end)  
                    end) as ModerateOrSevereLiverOrRenalDisease
            , (case when (stay_interval < 2 and (fs_MetastaticSolidTumor + ss_MetastaticSolidTumor) > 0) then 1 
                    else (case when fs_MetastaticSolidTumor > 1 then 1 else fs_MetastaticSolidTumor end)  
                    end) as MetastaticSolidTumor
        from
        ( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
            select --select with priority QNXT > CLM > ENC and join with MEMMO data
                FS.case_id as fs_case_id, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.cin_no, FS.member_no
                , FS.case_dx1, FS.case_dx2, FS.case_dx3, FS.case_dx4, FS.case_dx5, FS.case_dx6, FS.case_dx7, FS.case_dx8, FS.case_dx9, FS.case_dx10
                , FS.case_dx11, FS.case_dx12, FS.case_dx13, FS.case_dx14, FS.case_dx15, FS.case_dx16, FS.case_dx17, FS.case_dx18, FS.case_dx19, FS.case_dx20 
                , FS.case_pr1, FS.case_pr2, FS.case_pr3, FS.case_pr4, FS.case_pr5, FS.case_pr6, FS.case_pr7, FS.case_pr8, FS.case_pr9, FS.case_pr10
                , FS.pregnancy as fs_pregnancy, SS.pregnancy as ss_pregnancy, FS.chemo, FS.rehab, FS.transplant
                , FS.PreviousMyocardialInfarction as fs_PreviousMyocardialInfarction
                , SS.PreviousMyocardialInfarction as ss_PreviousMyocardialInfarction
                , FS.CerebrovascularDisease as fs_CerebrovascularDisease
                , SS.CerebrovascularDisease as ss_CerebrovascularDisease
                , FS.PeripheralVascularDisease as fs_PeripheralVascularDisease
                , SS.PeripheralVascularDisease as ss_PeripheralVascularDisease
                , FS.DiabetesWithoutComplications as fs_DiabetesWithoutComplications
                , SS.DiabetesWithoutComplications as ss_DiabetesWithoutComplications
                , FS.CongestiveHeartFailure as fs_CongestiveHeartFailure
                , SS.CongestiveHeartFailure as ss_CongestiveHeartFailure
                , FS.DiabetesWithEndOrganDamage as fs_DiabetesWithEndOrganDamage
                , SS.DiabetesWithEndOrganDamage as ss_DiabetesWithEndOrganDamage
                , FS.ChronicPulmonaryDisease as fs_ChronicPulmonaryDisease
                , SS.ChronicPulmonaryDisease as ss_ChronicPulmonaryDisease
                , FS.MildLiverOrRenalDisease as fs_MildLiverOrRenalDisease
                , SS.MildLiverOrRenalDisease as ss_MildLiverOrRenalDisease
                , FS.AnyTumor as fs_AnyTumor
                , SS.AnyTumor as ss_AnyTumor
                , FS.Dementia as fs_Dementia
                , SS.Dementia as ss_Dementia
                , FS.ConnectiveTissueDisease as fs_ConnectiveTissueDisease
                , SS.ConnectiveTissueDisease as ss_ConnectiveTissueDisease
                , FS.AIDS as fs_AIDS
                , SS.AIDS as ss_AIDS
                , FS.ModerateOrSevereLiverOrRenalDisease as fs_ModerateOrSevereLiverOrRenalDisease
                , SS.ModerateOrSevereLiverOrRenalDisease as ss_ModerateOrSevereLiverOrRenalDisease
                , FS.MetastaticSolidTumor as fs_MetastaticSolidTumor
                , SS.MetastaticSolidTumor as ss_MetastaticSolidTumor
                , FS.severity, FS.aprdrg, FS.dis_status, FS.provider
                , FS.cur_pcp, FS.cur_site_no, FS.product_code, FS.segment, FS.yearmth
                , FS.from_er
                , cast(FS.source_table as varchar(1)) as fs_source_table
                , SS.dis_dt as ss_dis_dt
                , concat(cast(FS.source_table as varchar(1)), ', ', cast(SS.source_table as varchar(1))) as ss_source_table
                , concat(FS.case_id, ', ', SS.case_id) as ss_case_id
                , case
                    when FS.cin_no = SS.cin_no 
                        then abs(datediff(SS.adm_dt, FS.dis_dt))
                        else null
                    end as stay_interval
                , FS.paid_amt_case as fs_paid_amt_case
                , SS.paid_amt_case as ss_paid_amt_case
            from
            NATHALIE.NJB_COMPLETE_CASES_6 as FS 
            inner join
            NATHALIE.NJB_COMPLETE_CASES_6 as SS
            ON SS.rownumber = FS.rownumber + 1
        ) AS INTERVAL_ADDED
    ) AS DIS_DT_UPDATED
) AS ROWNUMER_ADDED
where rownumber = 1
;

-- Check summary
select count(*)
        , sum(Pregnancy) as Pregnancy
        , sum(Chemo) as Chemo
        , sum(Rehab) as Rehab
        , sum(Transplant) as Transplant
        , sum(PreviousMyocardialInfarction) as PreviousMyocardialInfarction
        , sum(CerebrovascularDisease) as CerebrovascularDisease
        , sum(PeripheralVascularDisease) as PeripheralVascularDisease
        , sum(DiabetesWithoutComplications) as DiabetesWithoutComplications
        , sum(CongestiveHeartFailure) as CongestiveHeartFailure
        , sum(DiabetesWithEndOrganDamage) as DiabetesWithEndOrganDamage
        , sum(ChronicPulmonaryDisease) as ChronicPulmonaryDisease
        , sum(MildLiverOrRenalDisease) as MildLiverOrRenalDisease
        , sum(AnyTumor) as AnyTumor
        , sum(Dementia) as Dementia
        , sum(ConnectiveTissueDisease) as ConnectiveTissueDisease
        , sum(AIDS) as AIDS
        , sum(ModerateOrSevereLiverOrRenalDisease) as ModerateOrSevereLiverOrRenalDisease
        , sum(MetastaticSolidTumor) as MetastaticSolidTumor
from nathalie.njb_complete_cases_5;
--776089	141103	1488	4045	0	0	0	0	0	0	0	0	0	0	4834	0	0	0	0


/* 
add rownumber to the final product 
*/
drop table if exists NATHALIE.njb_complete_cases_6;

create table NATHALIE.njb_complete_cases_8 as
select 
    case_id
    , adm_dt
    , dis_dt
    , cin_no
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , pregnancy, chemo, rehab, transplant
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , paid_amt_case
    , row_number() over (order by cin_no asc, adm_dt asc, dis_dt asc) as rownumber
from NATHALIE.njb_complete_cases_7
;

/* 
count rows to see whether subsequent counts become lower 
*/

--select count(*) from  njb_complete_cases_4;
select count(*) from  njb_complete_cases_6;
--ANS:1,272,272; 1,270,709; 1,270,693; 1270692; [see 'last row' issue.]
--~to v6

/*
Save the last iteration
*/
drop table if exists nathalie.njb_transfers_absorbed;

create table nathalie.njb_transfers_absorbed
as
select *
from njb_complete_cases_8
;

/*
ELIMINATE CASES BASED ON HEDIS VALUE SETS
*/
-- Step 1: include acute & non acute days, but exclude stays that were solely non-acute --TK verify your operationalization of this step
-- Step 2: acute-to-acute transfers: done under "transfer absorbed". -- TK verify operational definition of acute vs. non-acute.
--          Done with SRV_CAT = '01ip_a' for all three case tables
-- Step 3: exclude hospital stays where index adm and dis dates are same. 
--          Done here on all admissions but may need to revisit logic so as to only exclude index admission and not readmits for this reason. TK
--          Note: at deployment cannot differenetiate based on length of index stay
            --create table nathalie.njb_hedis_step3 as
            --select *
            --from nathalie.njb_transfers_absorbed
            --where adm_dt != dis_dt
            --;
            --401039 out of 1270693 are eliminated
-- Step 4:
-- death during stay: --> eliminated after demographics are included so that deathdate may be used as additional filter
-- a principal diagnosis of pregnancy or of a condition originating in the perinatal period. 
drop table if exists nathalie.njb_no_pregnancy;

create table nathalie.njb_no_pregnancy
as
select *
from nathalie.njb_transfers_absorbed
where pregnancy = 0 or pregnancy is null
;

--some pregnancy values are null. REASON: To do with left joining logic when creating njb_unique_cases_2

-- Step 5:
-- Exclude index hospital stays where value_set_name include "Chemotherapy"
-- Exclude index hospital stays where value_set_name include "Rehabilitation"
-- Exclude index hospital stays where value_set_name include "Transplant"
-- [not done: Exclude index hospital stays where value_set_name include "Potentially Planned Procedures" without "Acute Condition" (no need since all non-acute already removed)]
drop table if exists nathalie.njb_hedis_compliant;

create table nathalie.njb_HEDIS_compliant
as
select *
from nathalie.njb_no_pregnancy
where (chemo = 0 or chemo is null)
and (rehab = 0 or rehab is null)
and (transplant = 0 or transplant is null)
;

select count(*) from nathalie.njb_complete_cases_1;
--896125
select count(*) from nathalie.njb_complete_cases_2;
--896125
select count(*) from nathalie.njb_complete_cases_3;
--895698
select count(*) from nathalie.njb_complete_cases_4;
--895698
select count(*) from nathalie.njb_complete_cases_5;
--895691
select count(*) from nathalie.njb_complete_cases_6;
--895691
select count(*) from nathalie.njb_transfers_absorbed;
--895691
select count(*) from nathalie.njb_no_pregnancy;
--741695
select count(*) from nathalie.njb_hedis_compliant;
--734832

/*
-- Add in Days_ER
create table nathalie.njb_HEDIS_ER
as
select h.*, er.ER_dos_1, er.Days_ER
from nathalie.njb_HEDIS_compliant h
left join ( select cin_no, case_id, count(distinct cl_id) as num_cl_id
                    , min(dos_1) as dos_1, min(admit_dt_clm) as admit_dt_clm_1, max(disch_dt_clm) as disch_dt_clm_last
                     , min(ER_dos_1) as ER_dos_1, sum(Days_ER) as Days_ER
            from NATHALIE.QS_UNIQUE_HDR_DET
            group by cin_no, case_id) er on h.cin_no = er.cin_no and h.case_id = er.case_id
;
*/

-- Add ER visits withiin 6-month to the index hospitalization (for LACE)
-- Combine all ER in-hospital stays from QNXT, CLM and ENC
-- Dedup through source_table (QNXT>CLM>ENC), and case_id (remove null if possible as Null is sorted in lower priority)
drop table if exists NATHALIE.QS_ER_DETAIL;

create table NATHALIE.QS_ER_DETAIL as
select *
from (select cin_no, case_id, cl_id, ER_dos_1, ER_dos_last, source_table, datediff(ER_dos_last, ER_dos_1) as ERDays
         , row_number() over(partition by cin_no, ER_dos_1 order by source_table asc, case_id asc) as rownumber
from (  select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm
            , min(hdr.admit_dt_clm) as ER_dos_1
            -- QNXT does not have ER records in detail table, using header table instead, but 164466 missing hdr.disch_dt_clm
            -- Plug QNXT header table disch_dt_clm missing value with max of thru_dt in detail file for the same cin_no and cl_id
            , max(case when hdr.disch_dt_clm is not null then hdr.disch_dt_clm else det.thru_dt end) as ER_dos_last
--            , avg(datediff(hdr.disch_dt_clm, hdr.admit_dt_clm)) as Days_ER
            , 1 as source_table
        from `hoap`.`qnxt_hdr_inpsnf` hdr join `hoap`.`qnxt_detail_inpsnf` det
        on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
        where substr(hdr.type_bill,1,2) in ('11','12')
        and hdr.adm_type in ('1','2')
        group by hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm
        union
        select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm
            , det.dos as ER_dos_1
            , case when det.thru_dt is not null then det.thru_dt else hdr.disch_dt_clm end as ER_dos_last
--            , case when datediff(det.dos, hdr.admit_dt_clm)>=0 and datediff(hdr.disch_dt_clm, det.thru_dt)>=0 
--                then datediff(det.thru_dt, det.dos) else 0 end as Days_ER
            , 2 as source_table
        from `hoap`.`clm_hdr_inpsnf` hdr join `hoap`.`clm_detail_inpsnf` det 
        on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
        where substr(type_bill,1,2) in ('11','12')
        and det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459')
        union
        select hdr.cin_no, hdr.case_id, hdr.cl_id, hdr.dos_1, hdr.admit_dt_clm, hdr.disch_dt_clm
            , det.dos as ER_dos_1
            , case when det.thru_dt is not null then det.thru_dt else hdr.disch_dt_clm end as ER_dos_last
--            , case when datediff(det.dos, hdr.admit_dt_clm)>=0 and datediff(hdr.disch_dt_clm, det.thru_dt)>=0 
--                then datediff(det.thru_dt, det.dos) else 0 end as Days_ER
            , 3 as source_table
        from `hoap`.`enc_hdr_inpsnf` hdr join `hoap`.`enc_detail_inpsnf` det 
        on hdr.cin_no=det.cin_no and hdr.cl_id=det.cl_id
        where substr(type_bill,1,2) in ('11','12')
        and det.rev_cd in ('0450', '0451', '0452', '0453', '0454', '0455', '0456', '0457', '0458', '0459')
) as ALL_ER
order by cin_no, ER_dos_1, case_id, er_dos_last, source_table
--where year(dos_1)=2017
) as ALL_ER_SORTED
where rownumber=1
;

-- Check missing dates
select source_table, sum(case when ER_dos_1 is null then 1 else 0 end), sum(case when ER_dos_last is null then 1 else 0 end)
from NATHALIE.QS_ER_DETAIL
group by source_table
order by source_table;

-- Check duplications in njb_HEDIS_compliant input table
select cin_no, case_id, adm_dt, count(*)
from nathalie.njb_HEDIS_compliant
group by cin_no, case_id, adm_dt
having count(*)>1
order by count(*) desc;
--TK: 54

-- Get all ER visits within the six-month window prior to index admission date
-- Note: ER visits with missing (null) thru_dt in DETAIL table (discharge date) will be ignored
drop table nathalie.njb_hedis_er_2;

create table nathalie.njb_hedis_er_2 as
select w.cin_no, w.case_id, w.adm_dt, w.dis_dt
        , sum(case when (datediff(w.adm_dt, er.er_dos_1) <= 183 and datediff(w.adm_dt, er.er_dos_1) >= 1)
                    or (datediff(w.adm_dt, er.er_dos_last) <= 183 and datediff(w.adm_dt, er.er_dos_last) >= 1)
                    then 1 else 0 end) as ER_visits
        , sum(case when (datediff(w.adm_dt, er.er_dos_1) <= 183 and datediff(w.adm_dt, er.er_dos_1) >= 1)
                    or (datediff(w.adm_dt, er.er_dos_last) <= 183 and datediff(w.adm_dt, er.er_dos_last) >= 1)
                    then least(datediff(w.adm_dt, er.er_dos_1), 183) - greatest(datediff(w.adm_dt, er.er_dos_last), 1) else 0 end) as ER_Days
from nathalie.njb_HEDIS_compliant w
left join NATHALIE.QS_ER_DETAIL er on w.cin_no = er.cin_no
group by w.cin_no, w.case_id, w.adm_dt, w.dis_dt
;

-- Check results by members
select count(distinct cin_no) as members
        , sum(er_visits) as er_visits, sum(er_days) as er_days
from nathalie.njb_hedis_er_2;

-- Check results by cases
select count(distinct case_id) as cases
        , sum(er_visits) as er_visits, sum(er_days) as er_days
from nathalie.njb_hedis_er_2;

-- Merge into index cases
drop table if exists nathalie.njb_hedis_compliant_er;

create table nathalie.njb_hedis_compliant_er as
select c.*, er.ER_visits, er.Days_ER
from nathalie.njb_hedis_compliant c
inner join nathalie.njb_hedis_er_2 er on c.cin_no = er.cin_no and c.case_id = er.case_id and c.adm_dt = er.adm_dt
;

/*
-- Missing case_id
drop table qs_tmp_dup_caseid
create table qs_tmp_dup_caseid as
select * from qs_er_detail where case_id is null
--658
*/

/*
BEGIN SANDBOX
*/
/*
COMORBIDITIES
Previous myocardial infarction 
Cerebrovascular disease 
Peripheral vascular disease Boolean
Diabetes without complications
Congestive heart failure
Diabetes with end organ damage
Chronic pulmonary disease
Mild liver or renal disease
Any tumor (including lymphoma or leukemia)
Dementia
Connective tissue disease
AIDS
Moderate or severe liver or renal disease
Metastatic solid tumor

HEDIS FLAGS
Pregnacy & Perinatal Period FLAG
REHABILITATION FLAG
TRANSPLANT FLAG

*/
/*
--Bring in the HEDIS value set table and recode as done in SAS script 
create table nathalie.njb_hedis as
select 
    *
    , case 
        when value_set_name = 'Chemotherapy' then 'chemo'
        when value_set_name = 'Rehabilitation' then 'rehab'
        when value_set_name = 'Transplant' and code_system in ('ICD9CM', 'ICD10CM') then 'transplant_dx'
        when value_set_name = 'Transplant' and code_system in ('ICD9PCS', 'ICD10PCS') then 'transplant_px'
        when value_set_name = 'Transplant' and code_system not in ('ICD9CM', 'ICD10CM', 'ICD9PCS', 'ICD10PCS') then 'transplant_ot'
        when value_set_name = 'Potentially Planned Procedures' then 'plan'
        when value_set_name = 'Acute condition' then 'acute'
        when value_set_name in ('Pregnancy', 'Perinatal Conditions') then 'pregnancy'
    end as fmtname
from flatfile.hedis_readmission_value_sets_2016;
ALT
create table nathalie.njb_hedis as
select 
    *
from flatfile.hedis_readmission_value_sets_2016
where code_system in ('ICD9CM', 'ICD10CM', 'ICD9PCS', 'ICD10PCS')
and value_set_name in ('Chemotherapy', 'Rehabilitation', 'Transplant', 'Potentially Planned Procedures', 'Acute condition', 'Pregnancy', 'Perinatal Conditions')
;
*/
/*
END SANDBOX
*/









--TK QING: CAN YOU RUN THE BLOCKS OF CODE BELOW TO GENERATE THE TABLES?



/*
INDEX vs READMITS: for each case, is it preceded by an index readmission within 30 days or not? Label 'I' if no, and 'R' if yes. 
*/

/*
--Compute labeled outcome: is index only or is readmit (I vs R)
--TK !!! pad the 1st and last rows of 2 versions of NATHALIE.NJB_[input name]. This will help with the next step where the tables are joined by an offset row number.
*/

--Add a dummy row to a copy of nathalie.njb_transfers_absorbed called njb_A, whose rownumber is 0, dis_dt/adm_dt are 100 years in past, and other vals are null
--Use thus to solve problem of sheared row on next steps
create table NATHALIE.njb_A as
select * from NATHALIE.njb_transfers_absorbed;

insert into NATHALIE.njb_A (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0); 

--join table with offset 1 and save values for latest admit per row, plus data concerning earliest admit that indicates whether target is Index (only) vs. Readmit
create table NATHALIE.njb_labeled_as_readmits as
SELECT 
    case_id
    , adm_dt
    , dis_dt
    , a_dis_dt -- spot check
    , cin_no
    , a_cin_no -- spot check
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , rownumber
    , days_since_prior_discharge
    , prior_stay_case_id
    , case
        when days_since_prior_discharge <= 30 then 1 
        else 0 
      end as is_a_30d_readmit
    , paid_amt_case
FROM
(
	SELECT 
    	  B.case_id
        , B.adm_dt
        , B.dis_dt
        , A.dis_dt as a_dis_dt -- spot check
        , B.cin_no
        , A.cin_no as a_cin_no -- spot check
        , B.member_no
        , B.case_dx1, B.case_dx2, B.case_dx3, B.case_dx4, B.case_dx5, B.case_dx6, B.case_dx7, B.case_dx8, B.case_dx9, B.case_dx10
        , B.case_dx11, B.case_dx12, B.case_dx13, B.case_dx14, B.case_dx15, B.case_dx16, B.case_dx17, B.case_dx18, B.case_dx19, B.case_dx20 
        , B.case_pr1, B.case_pr2, B.case_pr3, B.case_pr4, B.case_pr5, B.case_pr6, B.case_pr7, B.case_pr8, B.case_pr9, B.case_pr10
        , B.PreviousMyocardialInfarction, B.CerebrovascularDisease, B.PeripheralVascularDisease, B.DiabetesWithoutComplications
        , B.CongestiveHeartFailure, B.DiabetesWithEndOrganDamage, B.ChronicPulmonaryDisease, B.MildLiverOrRenalDisease
        , B.AnyTumor, B.Dementia, B.ConnectiveTissueDisease, B.AIDS, B.ModerateOrSevereLiverOrRenalDisease, B.MetastaticSolidTumor
        , B.severity
        , B.aprdrg
        , B.dis_status
        , B.provider
        , B.cur_pcp
        , B.cur_site_no
        , B.product_code
        , B.segment
        , B.yearmth
        , B.from_er
        , B.source_table
        , B.stay_interval
        , B.paid_amt_case
        , B.rownumber
    	, CASE
        		WHEN A.cin_no = B.cin_no THEN ABS(DATEDIFF(A.dis_dt, B.adm_dt))
        		ELSE NULL
    	    END AS days_since_prior_discharge
    	, case
        	    when A.cin_no = B.cin_no then A.case_id
        	    else null
        	end as prior_stay_case_id
    FROM NATHALIE.njb_A AS A LEFT JOIN NATHALIE.njb_transfers_absorbed AS B ON A.rownumber = B.rownumber - 1 -- A is earlier than B; 1st row for A is nulls/ancient dates
) AS S
;


/*
LABELED OUTCOME: for each case, is it followed by a readmission within 30 days or not
*/

/*
--Compute labeled outcome: readmit in 30 d, or no readmit (2), or death in 30 days
--TK !!! pad the 1st and last rows of 2 versions of NATHALIE.NJB_[input name]. This will help with the next step where the tables are joined by an offset row number.
*/

--Add a dummy row to a copy of nathalie.njb_transfers_absorbed called njb_A, whose rownumber is 0, dis_dt/adm_dt are 100 years in past, and other vals are null
--Use thus to solve problem of sheared row on next steps

create table NATHALIE.njb_A as
select * from NATHALIE.njb_labeled_as_readmits;

insert into NATHALIE.njb_A (adm_dt, dis_dt, rownumber)
values('1900-01-01', '1900-01-01', 0);

--main computation
create table NATHALIE.njb_labeled_outcomes as
SELECT 
    case_id
    , adm_dt
    , dis_dt
    , a_dis_dt -- spot check
    , b_dis_dt -- spot check
    , cin_no
    , a_cin_no -- spot check
    , b_cin_no -- spot check
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , rownumber
    , days_since_prior_discharge
    , prior_stay_case_id
    , is_a_30d_readmit
    , case
        when days_until_next_discharge <= 30 then 1 
        else 0 
      end as is_followed_by_a_30d_readmit
    , paid_amt_case
FROM
(
	SELECT 
    	  A.case_id
        , A.adm_dt
        , A.dis_dt
        , A.a_dis_dt -- spot check
        , B.dis_dt as b_dis_dt -- spot check
        , A.cin_no
        , A.a_cin_no
        , B.cin_no as b_cin_no -- spot check
        , A.member_no
        , A.case_dx1, A.case_dx2, A.case_dx3, A.case_dx4, A.case_dx5, A.case_dx6, A.case_dx7, A.case_dx8, A.case_dx9, A.case_dx10
        , A.case_dx11, A.case_dx12, A.case_dx13, A.case_dx14, A.case_dx15, A.case_dx16, A.case_dx17, A.case_dx18, A.case_dx19, A.case_dx20 
        , A.case_pr1, A.case_pr2, A.case_pr3, A.case_pr4, A.case_pr5, A.case_pr6, A.case_pr7, A.case_pr8, A.case_pr9, A.case_pr10
        , A.PreviousMyocardialInfarction, A.CerebrovascularDisease, A.PeripheralVascularDisease, A.DiabetesWithoutComplications
        , A.CongestiveHeartFailure, A.DiabetesWithEndOrganDamage, A.ChronicPulmonaryDisease, A.MildLiverOrRenalDisease
        , A.AnyTumor, A.Dementia, A.ConnectiveTissueDisease, A.AIDS, A.ModerateOrSevereLiverOrRenalDisease, A.MetastaticSolidTumor
        , A.severity
        , A.aprdrg
        , A.dis_status
        , A.provider
        , A.cur_pcp
        , A.cur_site_no
        , A.product_code
        , A.segment
        , A.yearmth
        , A.from_er
        , A.source_table
        , A.stay_interval
        , A.paid_amt_case
        , A.rownumber
        , A.days_since_prior_discharge
        , A.prior_stay_case_id
        , A.is_a_30d_readmit
    	, CASE
        		WHEN A.cin_no = B.cin_no THEN ABS(DATEDIFF(A.dis_dt, B.adm_dt))
        		ELSE NULL
    	    END AS days_until_next_discharge
    	, case
        	    when A.cin_no = B.cin_no then B.case_id
        	    else null
        	end as subsequent_stay_case_id
    FROM NATHALIE.njb_A AS A LEFT JOIN NATHALIE.njb_labeled_as_readmits AS B ON A.rownumber = B.rownumber - 1 -- A is earlier than B
) AS S
;

--because you kept as main case data from table 'A', which had a dummry rownumber = 1, you need to shear it off now from table njb_labeled_outcomes. 
--select * from 
delete from
njb_labeled_outcomes
where adm_dt = '1900-01-01'
;
--tk AnalysisException: Impala does not support modifying a non-Kudu table: nathalie.njb_labeled_outcomes // 


--spot check: ALL GOOD
/*
select dis_dt, a_dis_dt, b_dis_dt, cin_no, a_cin_no, b_cin_no, is_a_30d_readmit, is_followed_by_a_30d_readmit
from nathalie.njb_labeled_outcomes
limit 100
;
*/


/*
HEDIS step 3: exclude hospital stays where the index admission date is the same as the index doscharge date. ! Note: thais only applies to index admission, tnot to  readmission. 
*/
--TKTKTKTKTKTKTK







/*
do not use this block of code
BRING IN THE DEMOGRAPHICS: old = HOAP.MEMBERS table which turns out to be poorly populated & on its way out

    adm_dt
    , dis_dt
    , A.cin_no
    , A.member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , source_table
    , stay_interval
    , days_since_prior_discharge
    , case_id
    , prior_stay_case_id
    , 30d_readmit
    , paid_amt_case
    , dob
    , abs(datediff(dob, adm_dt)) as adm_age
    , gender
    , language_written_code
    , ethnicity_code
    , zip_code
    , zip4
    , deathdate

create table nathalie.njb_demographics_added
as
select 
    case_id
    , adm_dt
    , dis_dt
    , A.cin_no
    , A.member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , A.product_code
    , segment
    , yearmth
    , source_table
    , stay_interval
    , days_since_prior_discharge
    , prior_stay_case_id
    , is_a_30d_readmit
    , is_followed_by_a_30d_readmit
    , paid_amt_case
    , dob
    , abs(datediff(dob, adm_dt)) as adm_age --TK result is in days. Do datediff in years. 
    , gender
    , language_written_code
    , ethnicity_code
    , zip_code
    , zip4
    , deathdate
from nathalie.njb_labeled_outcomes as A left join hoap.members as B on A.cin_no = B.cin_no
;
*/

/*
BRING IN THE DEMOGRAPHICS: new = ENCPR.ENCOUNTER.MEMBERS 
+ is_a_30d_death: label members who died within 30 days of discharge (negative outcome)
AGE
TK Coding to do with age: Decide what ages to include for descriptive analyses and for prediction.
TK now: include all but segment for descriptives.  
Segmentation 1: 0-17; 18-64; 65+
Segmentation 2: Children (0-15), Transition Age Youth, aka TAY (16-25), Adult (26-59), Older Adults (60+)
DEATH
exclude members who died before discharge
*/

create table nathalie.njb_demographics_added
as
select
    case_id
    , adm_dt
    , dis_dt
    , cin_no
    , member_no
    , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
    , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
    , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
    , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
    , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
    , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
    , severity
    , aprdrg
    , dis_status
    , provider
    , cur_pcp
    , cur_site_no
    , product_code
    , segment
    , yearmth
    , from_er
    , source_table
    , stay_interval
    , days_since_prior_discharge
    , prior_stay_case_id
    , is_a_30d_readmit
    , is_followed_by_a_30d_readmit
    , paid_amt_case
    , dob
    , adm_age
    , case
            when adm_age is null then null
            when adm_age <= 17 then 'C' --child
            when adm_age > 17 and adm_age < 65 then 'A' --adult
            when adm_age >= 65 and adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_hedis 
    , case
            when adm_age is null then null
            when adm_age < 16 then 'C' --child
            when adm_age >= 16 and adm_age < 26 then 'T' --transition age adult, aka TAY
            when adm_age >= 26 and adm_age < 60 then 'A' --adult
            when adm_age >= 60 and adm_age <= 150 then 'O' --older adult
            else null
        end as agegp_cty 
    , gender
    , language_written_code
    , ethnicity_code
    , zip_code
    , zip4
    , phone
    , has_phone
    , deathdate
    , is_a_30d_death
from
(
    select 
        case_id
        , adm_dt
        , dis_dt
        , A.cin_no
        , A.member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , PreviousMyocardialInfarction, CerebrovascularDisease, PeripheralVascularDisease, DiabetesWithoutComplications
        , CongestiveHeartFailure, DiabetesWithEndOrganDamage, ChronicPulmonaryDisease, MildLiverOrRenalDisease
        , AnyTumor, Dementia, ConnectiveTissueDisease, AIDS, ModerateOrSevereLiverOrRenalDisease, MetastaticSolidTumor
        , severity
        , aprdrg
        , dis_status
        , provider
        , cur_pcp
        , cur_site_no
        , A.product_code
        , segment
        , yearmth
        , from_er
        , source_table
        , stay_interval
        , days_since_prior_discharge
        , prior_stay_case_id
        , is_a_30d_readmit
        , is_followed_by_a_30d_readmit
        , paid_amt_case
        , dob
        , case
                when dob is null then null
                else floor(datediff(adm_dt, dob) / 365.25) 
            end as adm_age 
        , gender
        , language_written_code
        , ethnicity_code
        , zip_code
        , zip4
        , phone
        , case
                when phone is null then 0
                else 1
            end as has_phone
        , deathdate
        , case
                when deathdate <= adddate(dis_dt, 30) then 1
                else 0
            end as is_a_30d_death
    from nathalie.njb_labeled_outcomes as A left join encpr.members as B on A.cin_no = B.cin_no
) as S
where dis_status not in ('20', '40', '41', '42') --member did not expire before discharge
and datediff(deathdate, dis_dt) >= 0 --member did not expire before discharge 
;

/*
LABEL THE LOBs and ENGINEER NEW NAMES (concatenate 10 + segment)
*/
/*Documentation*/
                    --FROM HOA DICTIONARY
                    IF
                    (HEADER_BP_CODE ='COMM'
                    AND PRODUCT_CODE='10')
                    THEN BP_CODE ='MCLA';
                    /*'COMMUNITY' OLD
                    RX VENDOR IN2011*/
                    ELSE IF (HEADER_BP_CODE
                    ='MEDI' AND
                    PRODUCT_CODE='10') THEN
                    BP_CODE ='MCLA';
                    /*'MEDIMPACT' OLD
                    RX VENDOR IN2014*/
                    ELSE IF (HEADER_BP_CODE
                    ='NAVI' AND
                    PRODUCT_CODE='10') THEN
                    BP_CODE ='MCLA';
                    /*'NAVI' NEW RX
                    VENDOR IN2015*/
                    ELSE IF (HEADER_BP_CODE
                    ='TRAN' AND
                    PRODUCT_CODE='10') THEN
                    BP_CODE ='MCLA';
                    /*'TRAN' NEW
                    HEADER BP CODE IN2017*/
                    ELSE IF (HEADER_BP_CODE
                    ='MHC' AND
                    PRODUCT_CODE='10') THEN
                    BP_CODE ='MCLA';
                    /*'MHC' NEW
                    HEADER BP CODE IN2017*/
                    ELSE IF (
                    PRODUCT_CODE='20
                    ') THEN BP_CODE ='HFP';
                    ELSE IF (
                    PRODUCT_CODE='40
                    ') THEN BP_CODE ='IHSS';
                    ELSE IF (
                    PRODUCT_CODE='60
                    ') THEN BP_CODE ='HKID';
                    ELSE IF (
                    PRODUCT_CODE='70
                    ') THEN BP_CODE ='SNP';
                    ELSE IF (
                    PRODUCT_CODE='80
                    ') THEN BP_CODE ='CMC';
                    ELSE IF (
                    PRODUCT_CODE='90
                    ') THEN BP_CODE ='HBEX';
                    ELSE BP_CODE =
                    HEADER_BP_CODE;

                    /*
                    Product_codes
                    10 = Medi-Cal
                    20 = Healthy Family Plan
                    40 = PASC-SEIU: Personal Assistance Services Counsel-Services Employees International Union /IHSS, In-Home Support Services
                    60 = Healthy Kids
                    70 = Dual Eligible Special Needs Plan
                    80 = Cal-Medi Connect
                    90 = LA Care Covered/Health Benefits Exchange
                    -1 = Other
                    */


--TK Need to apply labels to the dataset here for easier export to Excel & presentation of descriptive statistics


/*
CLEAN UP
DROP TABLE NATHALIE.NJB_ANALYTIC_SET;
*/
CREATE TABLE NJB_ANALYTIC_SET 
STORED AS PARQUET
AS
SELECT d.*, u.pregnancy, u.chemo, u.rehab, u.transplant
        , datediff(d.dis_dt,d.adm_dt) as LOS
FROM NATHALIE.NJB_DEMOGRAPHICS_ADDED d
left join nathalie.njb_unique_cases_2 u on d.cin_no=u.cin_no and d.case_id=u.case_id
where year(d.adm_dt)=2017
and datediff(d.dis_dt,d.adm_dt)>0
;

DROP TABLE NATHALIE.NJB_TRANSFERS_ABSORBED;
DROP TABLE NATHALIE.NJB_A;
DROP TABLE NATHALIE.NJB_LABELED_AS_READMITS;
DROP TABLE NATHALIE.NJB_LABELED_OUTCOMES;
DROP TABLE NATHALIE.njb_demographics_added;
DROP TABLE NATHALIE.NJB_ANALYTIC_SET;


/*
ANALYSES
BY LOB
count of admits, 
count of readmits, 
total cost admits
total cost of readmits
[note here that what I cannot capture is opportunity for intervention. Readmit will happen any way bc of illness type; 
see as added source of information maybe analyses of success in applying TOC --> do such analyses exist contrasting dif. Dx?]
*/

/*
TABULATE BY POPULATION and BY WHETHER IS A 1st/INDEX ADMIT (without previous admit within 30 days) vs. IS A READMIT
--look at counts and cost; plan on exclusing ENC / table 3 data where cost is null
*/

select 
    product_code --this ...
    , segment --...and this can be combined by case in order to produce LOBs. See code excerpts above (RE: "product_codes"; offset from Left)for guidance. 
    , source_table -- confirms that costs are only available where ENC / table 3 was *not* a source. 
    , is_a_30d_readmit
    , count(*)
    , sum(paid_amt_case)
from nathalie.njb_analytic_set
--where adm_age > 17 --TK filter is needed, but data field is poorly populated and reduces counts too much. Need to find alternative DOB source. 
--and adm_age < 65
group by product_code, segment, source_table, is_a_30d_readmit
order by product_code, segment, source_table, is_a_30d_readmit
;


/*
TABULATE BY POPULATION and BY WHETHER ADMISSION WOULD VS. WOULD NOT BE FOLLOWED BY READMITSSION WITHIN 30 DAYS
--look at counts alone; include ENC all the way.
*/

select 
    product_code --this ...
    , segment --...and this can be combined by case in order to produce LOBs. See code excerpts above (RE: "product_codes"; offset from Left)for guidance. 
    , is_followed_by_a_30d_readmit
    , count(*)
from nathalie.NJB_ANALYTIC_SET
--where adm_age > 17 --TK filter is needed, but data field is poorly populated and reduces counts too much. Need to find alternative DOB source. 
--and adm_age < 65
group by product_code, segment, is_followed_by_a_30d_readmit
order by product_code, segment, is_followed_by_a_30d_readmit
;





/*
REMAINING ISSUES
*/

--NATHALIE-- demographic SCREENING: death date -- SCREENING: birth date.

****--NATHALIE, bring on Qing for validation & support--TK4 Remove conditions according to HEDIS value sets. 

--TK5 filter to 1 year analysis

--LESLIE, QING, NATHALIE -- TK8 breakdown admit/readmit by Dx group (need to discover best grouping system) --> working with Leslie Seltzer on this. 

select count(distinct case_id) from nathalie.njb_analytic_set
select count(distinct case_id) from NATHALIE.QS_UNIQUE_HDR_DET

/*
--T10 recode analytic set for export to CDSW - 

Remove: case_id, [either member_no or cin_no], yearmth, source_table, prior_stay_case_id, dob, deathdate

Transform/engineering fodder: 
product_code+segment;
dx[1:20]  engineer superordinate nominal set; //maybe contanitate across the 20 fields and bring into R as 1 field, then separate after R ingestion. 
pr[1:10]  engineer TBD; 
provider  score indicating quality of provider, or latlon, or distance from member, or availability of systems TBD; 
cur_pcp  see provider; 
cur_site_no  lat lon, or distance to provider, or availability of public transportation, or accessibility for members with disabilities; 
language  match between provider and member on language; 

transform in R: 
--> dob  age; 
--> admt_dt and dis_dt  length of stay; season; day of the week; time of day; 

Translate into dummy vars: 
the superordinate Dx groups; 
the procedure groups; 
Dx + pr interactions; 
[aprdrg?], dis_status; 
product_code+segment; 
gender; 
language; 
ethnicity; 
zip code?

*/




/*
candidate predictor sets
*/

-- LACE: engineer length of stay; acuity of admission = severity; comorbidities; emergency department visits


select count(*) from nathalie.njb_analytic_set where adm_dt between '2017-01-01' and '2017-12-31';
select * from nathalie.njb_analytic_set limit 2;



-- tk ask Jordan for aprdrg dictionary 
select count(*), cast(sum(paid_amt_case) as int), aprdrg, product_code, segment
from nathalie.njb_labeled_as_readmits
where is_a_30d_readmit =  1
and adm_dt between '2017-01-01' and '2017-12-31'
group by aprdrg, product_code, segment
order by count(*) desc
;

select
from nathalie.njb_labeled_as_readmits


--TK this is a good candidate predictor
select from_er from hoap.clm_hdr_inpsnf limit 10; --