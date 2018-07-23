/*
POPULATION SELECTION CRITERION 1: MEMBER IS ALIVE AND CURRENTLY ENROLLED 

//DO NOT APPLY FOR MODEL TRAINING. 
//APPLY ONLY FOR MODEL DEPLOYMENT.

  Scoring can only benefit members who are currently enrolled. 

  CHOICE OF METHODS TO MEET CRITERION 1
  
      select distinct cin_no, pcp, site_no, spd, segment, product_code, yearmth
      from HOA.memmo
      where (cin_no, yearmth) in 
      ( -- select current members
        select *
        from
        ( -- select most recent valid membership month 
          select cin_no, max(yearmth) as yearmth
          from HOA.memmo
          where
          product_code in (10, 80) -- 10=MCLA. 80=CMC --***NEED TO UPDATE THIS
          and cin_no is not null
          --and substr(yearmth, 1, least(4, length(yearmth))) = '2017' --**note that a time frame is used
          group by cin_no
        )
        where yearmth in ('201802', '201803', '201804') --**note that a time frame is used
      )
    ;

  DECISION ON METHOD TO MEET CRITERION 1
  
  *Identify members in HOA.MEMMO whose most recent membership date is either current month, last month, or 2 months prior 
  (allows for late/appealed membership renewals as well as table refresh delay)

*/

-- see below for implementation




/*
POPULATION SELECTION CRITERION 2: MEMBER HAS BEEN CONTINUOUSLY ENROLLED FOR 12 MONTHS PERIOD USED TO PULL TRAINING VARIABLES 

--> Qing / Brandon agree instead to the following alternative:
alternative: include all members, flagthose for whom enrollment gaps prevent the conputation of a count of prior admissions or urgent visits

//DO NOT APPLY FOR MODEL DEPLOYMENT. 
//APPLY ONLY FOR MODEL TRAINING.
//ALTER SO THAT PERIOD IS DEFINED RELATIVE TO INDEX ADMISSION DATE

  Training is improved if gaps in membership do not skew representations of a member's care history. For instance,
  accurate counts of #hospitalizations and #urgent visits over prior 6-12 months lead to better model development.

  CHOICE OF METHODS TO MEET CRITERION 2
  
    select cin_no, count (*) as num_enrolled_months
    from
    (
      select distinct cin_no, yearmth
      from HOA.MEMMO
      where substr(yearmth, 1, least(4, length(yearmth))) in ('2017', '2018')
    )
    group by cin_no
    ;
  
  DECISION ON METHOD TO MEET CRITERION 2
  
  *Identify members in HOA.MEMMO whose membership had no gaps during the data capture period. 

*/

-- see below for implementation




/*
POPULATION SELECTION CRITERION 3: HIGH LIKELIHOOD OF HAVING HRA SCORE

  CHOICE OF METHODS TO MEET CRITERION 3
  
  *contrast modeling with vs. without HRA scores (A)
  or
  *pick w/HRA score for initial iteration (B)
  
  *method of picking a subpopulation (goes with B above)
  --Members with HRA score
  --Selected all MCLA and CMC members and note members who are SPD. 
    -- use HOA MEMMO table to filter by SPD/MCLA 
    -- use QNXT. Logic from Brandon: lines 113:121 of the linked script, it uses the `ratecode` fielf from QNXT enrollkeys table 10:32 AM; https://dsghe.lacare.org/bshelton/cob_logic_temp/blob/master/cob_pmpm_calcs.R See Jabber 20180412
    -- use FAME. filter by SPD/MCLA using logic provided by Kenyon in emails dated 20180412
    
    WHY SELECT THESE SUBPOPS? CMC, and SPD as segment (not LACC, CCI, TANF, MCE/medical expansion…). Thee reasoning is that HRA scores 
    are available for these members. See conversation with Scarlett Noguero. Also, Matt Pirritabo: "[W]e really only call people SPD in 
    Medi-Cal. SPD exists as a segment in Medi-Cal. Folks in CMC might also have an aid code that puts them in the SPD bucket but we don’t 
    typically refer to CMC members as SPD- they  are just CMC." 
  
        Method to identify CMC and MCLA is to use product codes. A product code table exists in ENCPR.ENCOUNTER table,
        in the HOA Data Dictionary, and in HOA table PRODUCT_CODES
        select *
        from HOA.PRODUCT_CODES
        ;
        code = 10 for MCLA and code = 80 for CMC
        
  DECISION ON METHOD TO MEET CRITERION 3
  
  *Identify all members with product code 80 or all with segment = 'SPD' in HOA.MEMMO table. 
  
  ***JUSTIFICATION: need some sort of quantitative justification for why we're going after this group
  --number of readmit
  --cost for readmits
  --> relative to other groups
  
  ***** TK Need this analyis before stakeholder meeting
  
*/
/*
--Find most recent enrolled month for MCLA and SPD members in HOAP.MEMMO. 
select distinct cin_no, pcp, site_no, product_code, segment, yearmth
from HOA.memmo
where (cin_no, yearmth) in 
( -- select most recent valid membership month 
  select cin_no, max(yearmth) as yearmth
  from HOA.memmo
  where
  (
    product_code = 80 -- CMC
    or
    segment = 'SPD'
  )
  and cin_no is not null
  group by cin_no
)
and --ensure inclusion is limited to members currently in CMC or SPD-segment  and not recategorized since
(
  product_code = 80 -- CMC
  or
  segment = 'SPD'
)
;
*/



/*
POPULATION SELECTION CRITERION 4: MEMBER COMPLETED AT LEAST ONE INPATIENT STAY DURING MODEL-TRAINING TIME FRAME 

  DECISION ON METHOD TO MEET CRITERION 4
  
  * Identify members in QNXT, CLM and ENC tables with at least 1 unique tupple (member_no, admit_date, discharge_date)
  * Duplicate admission records are removed. Priority is QNXT > CLM > ENC
  
*/
/*
select * --select with priority QNXT > CLM > ENC
from
(
  select case_id, adm_dt, dis_dt, cin_no, member_no
  , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
  , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
  , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
  , severity, aprdrg, dis_status, provider
  --table_source and rownum below rank QNXT > CLM > ENC sources
  , table_source
  , rownum()
    over (partition by member_no, adm_dt, dis_dt
          order by table_source asc) as rownumber
  from
  (      
      --select data from QNXT cases with distinct (member_no, adm_dt, dis_dt) tupples
      select *
      from
      (
        select case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider
        , 1 as table_source
        , rownum()
          over (partition by member_no, adm_dt, dis_dt
                order by case_id desc) as rownumber
        from HOA.QNXT_CASE_INPSNF
      )
      where rownumber =  1
      union
      --select data from CLM cases that did not appear in QNXT
      select *
      from
      (
        select case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider
        , 2 as table_source
        , rownum()
          over (partition by member_no, adm_dt, dis_dt
                order by case_id desc) as rownumber
        from HOA.CLM_CASE_INPSNF
      )
      where rownumber =  1
      union
      select *
      from
      (
        select case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider
        , 3 as table_source
        , rownum()
          over (partition by member_no, adm_dt, dis_dt
                order by case_id desc) as rownumber
        from HOA.ENC_CASE_INPSNF
      )
      where rownumber =  1
  )
  where rownumber =  1 
)
;
*/

/*
DATA CLEANING STEP: ACUTE-TO-ACUTE TRANSFERS

If the discharge date is on the same day or 1 day before the next admission date, they are considered transfers. 
For transfers, use the diagnosis codes from the first admission. Use rge last admission for the consolidated discharge date. 
  
*/

--combine POPULATION SELECTION CRITERION 1 with POPULATION SELECTION CRITERION 4 and then modify the results as follows





--The result is a selection of all unique and complete stays at inpatient facilities by members who were most recently in CMC or who were SPD in MCLA





select 
  case_id
  , adm_dt
  , case
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
  , table_source  

from

( --'INTERVAL_ADDED' // add interval between 1st discharge date and 2nd admit date to create subquery called 'interval_added'
    select --select with priority QNXT > CLM > ENC and join with MEMMO data
      , FS.case_id, FS.adm_dt, FS.dis_dt as fs_dis_dt, FS.cin_no, FS.member_no
      , FS.case_dx1, FS.case_dx2, FS.case_dx3, FS.case_dx4, FS.case_dx5, FS.case_dx6, FS.case_dx7, FS.case_dx8, FS.case_dx9, FS.case_dx10
      , FS.case_dx11, FS.case_dx12, FS.case_dx13, FS.case_dx14, FS.case_dx15, FS.case_dx16, FS.case_dx17, FS.case_dx18, FS.case_dx19, FS.case_dx20 
      , FS.case_pr1, FS.case_pr2, FS.case_pr3, FS.case_pr4, FS.case_pr5, FS.case_pr6, FS.case_pr7, FS.case_pr8, FS.case_pr9, FS.case_pr10
      , FS.severity, FS.aprdrg, FS.dis_status, FS.provider  
      , SS.dis_dt as ss_dis_dt
      , CONCAT(FS.table_source, SS.table_source)  as table_source
      , case
        when FS.member_no = SS.member_no 
          then datediff(d, FS.dis_dt, SS.adm_dt)
          else null
        end as stay_interval

    from
      
    ( --FIRST STAY 'FS'
      select 
       row_number() over (order by member_no asc, adm_dt asc, dis_dt asc) as rownumber
      , case_id, adm_dt, dis_dt, cin_no, member_no
      , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
      , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
      , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
      , severity, aprdrg, dis_status, provider
      --table_source and rownum below rank QNXT > CLM > ENC sources
      , table_source
      , pcp, site_no, product_code, segment, yearmth
      from
      (      
          --select data from QNXT cases with distinct (member_no, adm_dt, dis_dt) tupples
          select *
          from
          (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 1 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                    order by case_id desc) as rownumber
              from HOA.QNXT_CASE_INPSNF
          )
          where rownumber =  1
          union
          --select data from CLM cases that did not appear in QNXT
          select *
          from
          (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 2 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                    order by case_id desc) as rownumber
              from HOA.CLM_CASE_INPSNF
          )
          where rownumber =  1
          union
          select *
          from
          (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 3 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                    order by case_id desc) as rownumber
              from HOA.ENC_CASE_INPSNF
          )
          where rownumber =  1
      ) A
      left join
      (
        select distinct cin_no, pcp, site_no, product_code, segment, yearmth
        from HOA.memmo
        where (cin_no, yearmth) in 
        ( -- select most recent valid membership month 
          select cin_no, max(yearmth) as yearmth
          from HOA.memmo
          where
          (
            product_code = 80 -- CMC
            or
            segment = 'SPD'
          )
          and cin_no is not null
          group by cin_no
        )
        and --ensure inclusion is limited to members currently in CMC or SPD-segment  and not recategorized since
        (
          product_code = 80 -- CMC
          or
          segment = 'SPD'
        )
      ) B
      on A.cin_no = B.cin_no
      order by rownumber
    ) AS FS      
  
    INNER JOIN
  
    ( --SECOND STAY 'SS'
      select 
          r o w _ n u m b e r ( )   o v e r   ( o r d e r   b y  member_no  a s c ,  adm_dt  a s c ,  dis_dt  a s c )   a s   r o w number
        , case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider, table_source  
        , pcp, site_no, product_code, segment, yearmth
      from
      (
        select case_id, adm_dt, dis_dt, cin_no, member_no
        , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
        , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
        , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
        , severity, aprdrg, dis_status, provider
        --table_source and rownum below rank QNXT > CLM > ENC sources
        , table_source
        , rownum()
          over (partition by member_no, adm_dt, dis_dt
                order by table_source asc) as rownumber
        from
        (      
            --select data from QNXT cases with distinct (member_no, adm_dt, dis_dt) tupples
            select *
            from
            (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 1 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                      order by case_id desc) as rownumber
              from HOA.QNXT_CASE_INPSNF
            )
            where rownumber =  1
            union
            --select data from CLM cases that did not appear in QNXT
            select *
            from
            (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 2 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                      order by case_id desc) as rownumber
              from HOA.CLM_CASE_INPSNF
            )
            where rownumber =  1
            union
            select *
            from
            (
              select case_id, adm_dt, dis_dt, cin_no, member_no
              , case_dx1, case_dx2, case_dx3, case_dx4, case_dx5, case_dx6, case_dx7, case_dx8, case_dx9, case_dx10
              , case_dx11, case_dx12, case_dx13, case_dx14, case_dx15, case_dx16, case_dx17, case_dx18, case_dx19, case_dx20 
              , case_pr1, case_pr2, case_pr3, case_pr4, case_pr5, case_pr6, case_pr7, case_pr8, case_pr9, case_pr10
              , severity, aprdrg, dis_status, provider
              , 3 as table_source
              , rownum()
                over (partition by member_no, adm_dt, dis_dt
                      order by case_id desc) as rownumber
              from HOA.ENC_CASE_INPSNF
            )
            where rownumber =  1
        ) A
        left join
        (
          select distinct cin_no, pcp, site_no, product_code, segment, yearmth
          from HOA.memmo
          where (cin_no, yearmth) in 
          ( -- select most recent valid membership month 
            select cin_no, max(yearmth) as yearmth
            from HOA.memmo
            where
            (
              product_code = 80 -- CMC
              or
              segment = 'SPD'
            )
            and cin_no is not null
            group by cin_no
          )
          and --ensure inclusion is limited to members currently in CMC or SPD-segment  and not recategorized since
          (
            product_code = 80 -- CMC
            or
            segment = 'SPD'
          )
        ) B
        on A.cin_no = B.cin_no
        order by rownumber
    ) AS SS
    ON B.rownumber = A.rownumber + 1
  
) AS INTERVAL_ADDED

;

-- Focus on specific Dx
--TK SHOW RATES AND COSTS FOR DIFFERENT DX GROUPS

--age at time of index admission [18-64] 
--> effect on 'senior' part of SPD population??????????????????????????????????????????????????????????????????????
--TK DO AWAY WITH UPPER AGE LIMIT; TK analyze what is left out if we have a lower age limit. Contrast rates and cost for < 18 and 18+.
--dob in HOA dictionary is not found in indicated tables. select dob from hoa.enc_case_inpsnf where rownum<10; --> no result; field not in table any more

--continuous 12 months enrollment up to index admission date
--derive from tables above in separate query
--TK include regardless and flag gaps in enrollment that affect computation of certain candidate predictors

--either alive or readmitted 30 days post discharge (exclude if death occurs between index admit date and index discharge date + 30)
--death in SAS script only considers death while in hospital (dis_status in ('40', '41', '42', '20'). . Find own source
--TK TO BRING UP WITH STAKEHOLDERS AT PRESENTATION: MODEL THAT PREDICTS OUTCOME VS. MODEL THAT ALSO FOCUSES ON PREVENTABLE READMISSION.
--TK TO FILTER BY OR TO PREDICT WITH SOMETHING LIKE SEVERITY OR APRDRG = 9 VS LOWER

--inpatient stays to exclude SNF, correctional, christian science, just-ER visits [?????for index admission; but for readmit, include cost of ER visit]; what about index length of stay -- does it include ER stay?
--verify SAS coding and Mary Q.'s universe

--apply exclusion sets using Yonsu's excel spreadsheet
--must be done in platform. Have migrated Exclusion set spreadsheet from HEDIS 2016. Use CASE_DX against value set to determine eligibility.

--bring in totalpaid from HOA tables
select pd_amt
from hoa.clm_hdr_inpsnf 
where [member_no match, claim date inside span of case from case table]
and from_er [discuss????????????????? a readmission that is prevented may be one where ER bills are incurred]

--bring in the HRA scores (see Arun Sekar's code)
--Production Database Server \ Name:  SQL16TZGPRD2..CSNLACSQL06_CKOLTP
--select * from P_MEMBER_CONCEPT_VALUE where CONCEPT_id =504732 and cid = <<CCA Member Primary Key>>
--I still need to find the component scores.

--bring in demographic data (from where? Saleforce? Oracle tables? //-- see variable list)

--bring in CRG data from Yonsu's monthly flat file. Though select * from hoa.crg where rownum < 10; is populated, it was a one-time load in December 2017.


