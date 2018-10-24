/***
Title:              step5_PPG_LOB_PCP
Description:        Add PPG, Product, Segment, PCP assignment at time of admit 
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        nathalie.prjrea_step4_procedures
                    plandata.enrollkeys
                    edwp.vw_grp_cd_segmtn
                    plandata.eligibilityorg
                    plandata.affiliation
                    plandata.provider
                    HOAP.memmo
Output:             nathalie.prjrea_step5_PPG_LOB_PCP
Notes:              Can be improved by bringing in pcp info. Search below for /*TK HERE YOU HAVE OPPORTUNITY TO BRING IN PCP
***/

-- get enrollkey line with enrollid, ratecode, eff & term dates

drop table if exists nathalie.prjrea_step5_PPG_LOB_PCP;

create table nathalie.prjrea_step5_PPG_LOB_PCP 
as
select 
    A.*
    , seg.segment as segment
    , lob.lob as lob
    , lob.product_name as product_name
    , ppg.ppg as ppg
    , ppg.ppg_name as ppg_name
    , case 
            when ppg.ppg in ('AVHC','BHC','CTHC','DHHC','ELHC','EMCH',
                'ERCH','GPHC','HCHC','HDHS','HHHC','HUMC','HUMF','LACU',
                'LBCH','LCC','LLAC','LPHC','MLKH','OVMC','OVMV','RLAC',
                'SFHC','SGHC','SPHC','STR','THC','WIHC','WVCH') then 'DHS-PPG' 
            else 'Non-DHS PPG'
        end as DHS_site
    , case
            when ppg.ppg in ('AVHC','HDHS','LCC','LLAC','SPHC') then 'served by non-DHS facilities' --Shared risk / Antelope Valley PPG members typically go to non-DHS facilities. See email from Brandon Shelton Sent: Friday, August 31, 2018 2:02 PM
            when ppg.ppg in ('BHC','CTHC','DHHC','ELHC','EMCH',
                'ERCH','GPHC','HCHC','HDHS','HHHC','HUMC','HUMF','LACU',
                'LBCH','LPHC','MLKH','OVMC','OVMV','RLAC',
                'SFHC','SGHC','STR','THC','WIHC','WVCH') then 'served by DHS facilities'            
            else 'served by non-DHS facilities'
        end as DHS_service   
    , case
            when product_name='Cal-Medi Connect (CMC)' then 'Medi-Medi'
            when seg.segment in ('CCI') then 'Medi-Medi'
            when seg.segment in ('MCE', 'TANF', 'SPD') and (product_name<>'Cal_medi Connect (CMC)' or product_name is null) then 'Medicaid'
            when product_name='LA Care Covered/Health Benefits Exchange' then 'Commercial'
            when product_name in ('PASC-SEIU', 'Healthy Kids') then 'Commercial' --there has been no Healthy Kids data since 2016; out of scope for Medi-Cal (http://www.first5la.org/index.php?r=site/article&id=3088)
            else null
        end as NCQA_LOB
from nathalie.prjrea_step4_procedures as A
left join
(

    /*
    Get MCLA segment 
    Two methods are used, one that aligns with claims_universe and the other with EDW guidelines as I currently understand them.
    Priority is given to the EDW approach. Note also that I departed from the claims_universe approach by including segment values for any LOS and not just MCLA. 
    */

    select case_id, segment
    from 
    (
        -- select case_id, segment, row_number() over (partition by case_id order by priority) as rn
        select case_id, segment, row_number() over (partition by case_id order by priority) as rn
        from 
        (
        
            --1) via edwp.mthly_memshp; priority = '2'

            select case_id, segment, case when segment is not null then 2 else 3 end as priority
            from 
            (
                select 
                    Ca.case_id
                    , MEMMO.segment
                    , row_number() over(partition by Ca.case_id order by MEMMO.process_date desc) as rn
                from
                (
                    select cin_no, case_id, concat(cast(date_part('year', adm_dt) as varchar(4)), lpad(cast(date_part('month', adm_dt) as varchar(4)), 2, '0')) as adm_yearmth
                    from
                    nathalie.prjrea_step4_procedures 
                ) Ca 
                left join
                (
                    select sgmnt as segment, cin_no, mth_id as yearmth, updt_dt as process_date
                    from edwp.mthly_memshp
                ) as MEMMO
                on Ca.cin_no = MEMMO.cin_no
                and Ca.adm_yearmth = MEMMO.yearmth
            ) S
            where rn = 1

            union
            
            --2) via ENROLLKEY (for instances with multiple matching enrollkey rows, keep the enrollkeys row with the latest `lastupdate` and 'createdate' timestamps)

            select case_id, segment, case when segment is not null then 1 else 3 end as priority
            from   
            (
                select 
                    ca.case_id
                    , seg.segmtn as segment
                    , row_number() over (partition by ca.case_id order by ek.lastupdate desc, ek.createdate desc) as rn
                    -- where seg.lob='MCLA' and segment in ('CCI','MCE','TANF','SPD') -- same as in claims_universe; why limit to MCLA, though? Similar segment vals are found under BCBS, CFST, COMM, KAIS lobs
                from nathalie.prjrea_step4_procedures as ca
                left join
                (
                    select carriermemid, effdate, termdate, ratecode, createdate, lastupdate, planid, eligibleorgid, enrollid
                    from plandata.enrollkeys 
                    where segtype = 'INT' and ratecode <> 'CMCWELL'
                ) ek
                on ca.cin_no = ek.carriermemid
                and ca.adm_dt >= ek.effdate
                and ca.adm_dt <= ek.termdate
                left join edwp.vw_grp_cd_segmtn as seg
                on ek.ratecode = seg.grp_cd
            ) S
            where rn = 1
            
        ) seg_inner_1
        
    ) seg_inner_2
    
    where rn = 1 
    
) seg
on A.case_id = seg.case_id

left join  
(
   /* get LOB 
    Two methods are used, one that aligns with claims_universe and the other with EDW guidelines as I currently understand them.
    Priority is given to the EDW approach.
   */
   
    select case_id, lob, product_name
    from 
    (
        select case_id, lob, product_name, row_number() over (partition by case_id order by priority) as rn
        from 
        (

            -- --1) via MEMMO
            
            -- select case_id, lob, product_name, case when lob is null then 3 else 1 end as priority --if null, drop priority
            -- from 
            -- (
            --     select Ca.case_id
            --         -- , MEMMO.product_code as lob
            --         , case
            --             when MEMMO.product_code in ('90', 'COVERED CALIFORNIA', 'LA CARE COVERED DIRECT', 'LA Care Covered/Health Benefits Exchange') then 'LACC'
            --             when MEMMO.product_code in ('80', 'CMC SPONSOR', 'Cal-Medi Connect (CMC)') then 'CMC'
            --             when MEMMO.product_code in ('KAIS', 'KAISER PERMANENTE') then 'KAIS'
            --             when MEMMO.product_code in ('BCSC', 'ANTHEM BLUE CROSS OF CA MEDI-CAL') then 'BCSC'
            --             when MEMMO.product_code in ('CFST', 'CARE 1ST HEALTH PLAN MEDI-CAL') then 'CFST'
            --             when MEMMO.product_code in ('10', 'MCLA', 'MCLA-MCE', 'MCLA-CCI', 'MCLA-TANF', 'MCLA-SPD', 'MCLA') then 'MCLA'
            --             when MEMMO.product_code in ('40', 'PASC-SEIU') then 'PASC-SEIU'
            --             when MEMMO.product_code in ('60', 'Healthy Kids') then 'Healthy Kids'
            --             when MEMMO.product_code in ('Healthy Family Plan', 'Other', 'Dual Eligible Special Needs Plan') then MEMMO.product_code
            --             else null
            --           end as lob
            --         , lob.output as product_name
            --         , row_number() over(partition by Ca.case_id order by MEMMO.process_date desc) as rn
            --     from
            --     (
            --         select cin_no, case_id, concat(cast(date_part('year', adm_dt) as varchar(4)), lpad(cast(date_part('month', adm_dt) as varchar(4)), 2, '0')) as adm_yearmth
            --         from
            --         nathalie.prjrea_step4_procedures 
            --     ) Ca 
            --     left join
            --     (
            --         select product_code, cin_no, yearmth, process_date
            --         from HOAP.memmo
            --     ) as MEMMO
            --     on Ca.cin_no = MEMMO.cin_no
            --     and Ca.adm_yearmth = MEMMO.yearmth
            --     left join nathalie.ref_lob as lob
            --     on memmo.product_code = lob.input
            -- ) S
            -- where rn = 1

            --1) via edwp.mthly_memshp (replaces hoap.MEMMO); now has lower priority '2'
            
            select case_id, lob, product_name, case when lob is null then 3 else 2 end as priority
            from 
            (
                select Ca.case_id
                    -- , MEMMO.product_code as lob
                    , case
                        when MEMMO.product_code in ('90', 'COVERED CALIFORNIA', 'LA CARE COVERED DIRECT', 'LA Care Covered/Health Benefits Exchange') then 'LACC'
                        when MEMMO.product_code in ('80', 'CMC SPONSOR', 'Cal-Medi Connect (CMC)') then 'CMC'
                        when MEMMO.product_code in ('KAIS', 'KAISER PERMANENTE') then 'KAIS'
                        when MEMMO.product_code in ('BCSC', 'ANTHEM BLUE CROSS OF CA MEDI-CAL') then 'BCSC'
                        when MEMMO.product_code in ('CFST', 'CARE 1ST HEALTH PLAN MEDI-CAL') then 'CFST'
                        when MEMMO.product_code in ('10', 'MCLA', 'MCLA-MCE', 'MCLA-CCI', 'MCLA-TANF', 'MCLA-SPD', 'MCLA') then 'MCLA'
                        when MEMMO.product_code in ('40', 'PASC-SEIU') then 'PASC-SEIU'
                        when MEMMO.product_code in ('60', 'Healthy Kids') then 'Healthy Kids'
                        when MEMMO.product_code in ('Healthy Family Plan', 'Other', 'Dual Eligible Special Needs Plan') then MEMMO.product_code
                        else null
                      end as lob
                    , lob.output as product_name
                    , row_number() over(partition by Ca.case_id order by MEMMO.process_date desc) as rn
                from
                (
                    select cin_no, case_id, concat(cast(date_part('year', adm_dt) as varchar(4)), lpad(cast(date_part('month', adm_dt) as varchar(4)), 2, '0')) as adm_yearmth
                    from
                    nathalie.prjrea_step4_procedures 
                ) Ca 
                left join
                (
                    select lob_cd as product_code, cin_no, mth_id as yearmth, updt_dt as process_date
                    from edwp.mthly_memshp
                ) as MEMMO
                on Ca.cin_no = MEMMO.cin_no
                and Ca.adm_yearmth = MEMMO.yearmth
                left join nathalie.ref_lob as lob
                on memmo.product_code = lob.input
            ) S
            where rn = 1

            union

            --2) via ENROLLKEY and plandata. Now has higher priority
        
            select case_id
                , lob1 as lob
                , product_name
                , case when lob1 is null then 3 else 1 end as priority 
            from   
            ( 
                select 
                    ca.case_id
                    , case
                        when trim(eo.fullname) in ('COVERED CALIFORNIA', 'LA CARE COVERED DIRECT', 'LA Care Covered/Health Benefits Exchange') then 'LACC'
                        when trim(eo.fullname) in ('CMC SPONSOR', 'Cal-Medi Connect (CMC)') then 'CMC'
                        when trim(eo.fullname) in ('KAIS', 'KAISER PERMANENTE') then 'KAIS'
                        when trim(eo.fullname) in ('BCSC', 'ANTHEM BLUE CROSS OF CA MEDI-CAL') then 'BCSC'
                        when trim(eo.fullname) in ('CFST', 'CARE 1ST HEALTH PLAN MEDI-CAL') then 'CFST'
                        when trim(eo.fullname) in ('MCLA', 'MCLA-MCE', 'MCLA-CCI', 'MCLA-TANF', 'MCLA-SPD', 'MCLA') then 'MCLA'
                        when trim(eo.fullname) in ('Healthy Family Plan', 'PASC-SEIU', 'Healthy Kids', 'Other', 'Dual Eligible Special Needs Plan') then trim(eo.fullname)
                        else null
                      end as lob1
                    -- , seg.lob as lob2  -- results of this line are either in agreement with lob1 or are null. Therefore only lob1 is utilized. 
                    , case 
                        when trim(eo.fullname) in ('COVERED CALIFORNIA', 'LA CARE COVERED DIRECT', 'LA Care Covered/Health Benefits Exchange') then 'LA Care Covered/Health Benefits Exchange'
                        when trim(eo.fullname) in ('CMC SPONSOR', 'Cal-Medi Connect (CMC)') then 'Cal-Medi Connect (CMC)'
                        when trim(eo.fullname) in ('KAIS', 'KAISER PERMANENTE') then 'KAISER PERMANENTE'
                        when trim(eo.fullname) in ('BCSC', 'ANTHEM BLUE CROSS OF CA MEDI-CAL') then 'ANTHEM BLUE CROSS OF CA MEDI-CAL'
                        when trim(eo.fullname) in ('CFST', 'CARE 1ST HEALTH PLAN MEDI-CAL') then 'CARE 1ST HEALTH PLAN MEDI-CAL'
                        when trim(eo.fullname) in ('MCLA', 'MCLA-MCE', 'MCLA-CCI', 'MCLA-TANF', 'MCLA-SPD', 'MCLA') then 'MCLA'
                        when trim(eo.fullname) in ('Healthy Family Plan', 'PASC-SEIU', 'Healthy Kids', 'Other', 'Dual Eligible Special Needs Plan') then trim(eo.fullname)
                        else trim(eo.fullname)
                    end as product_name
                    , row_number() over (partition by ca.case_id order by ek.lastupdate desc, ek.createdate desc) as rn
                    -- where lob='MCLA' and segment in ('CCI','MCE','TANF','SPD') -- same as in claims_universe; why limit to MCLA, though? Similar segment vals are found under BCBS, CFST, COMM, KAIS lobs
                from nathalie.prjrea_step4_procedures as ca
                left join
                (
                    select carriermemid, effdate, termdate, ratecode, createdate, lastupdate, planid, eligibleorgid, enrollid
                    from plandata.enrollkeys 
                    where segtype = 'INT' and ratecode <> 'CMCWELL'
                ) ek
                on ca.cin_no = ek.carriermemid
                and ca.adm_dt >= ek.effdate
                and ca.adm_dt <= ek.termdate
                left join plandata.eligibilityorg as eo
                on ek.eligibleorgid = trim(eo.eligibleorgid)
                left join edwp.vw_grp_cd_segmtn as seg
                on ek.ratecode = seg.grp_cd
            ) S

        ) lob_inner_1
        
    ) lob_inner_2
    
    where rn = 1 
    
) lob
on A.case_id = lob.case_id


left join  
(
   /* get PPG 
    Two methods are used, one that aligns with claims_universe and the other with EDW guidelines as I currently understand them.
    Priority is given to the EDW approach.
   */

    select case_id, ppg, ppg_name
    from 
    (
        select case_id, ppg, ppg_name, row_number() over (partition by case_id order by priority) as rn
        from 
        (
 
            --1) via edwp.mem_prov_asgnmt_hist

            select case_id, ppg, ppg_name, case when ppg is null then 3 else 1 end as priority
            from 
            (
                select Ca.case_id
                    , PPG.PPG as PPG
                    , PPG.ppg as ppg_name --TK need to improve ppg_name under priority 1 block
                    , row_number() over(partition by case_id order by PPG.EFF_DT desc) as rownumber2
                from NATHALIE.PRJREA_STEP4_procedures as Ca
                left join 
                ( --Bring in PPG assignments
                    select distinct ppg, cin_no, eff_dt, term_dt
                    from 
                    (
                    	select A.ppg, B.carriermemid as cin_no, A.eff_dt, A.term_dt
                    	from edwp.mem_prov_asgnmt_hist as A
                    	left join 
                    	plandata.enrollkeys as B
                    	on A.MEM_BUS_KEY_NUM = B.memid
                    	where substr(A.MEM_BUS_KEY_NUM, 1, 3) = 'MEM'
                    	union 
                    	select ppg, MEM_BUS_KEY_NUM, eff_dt, term_dt
                    	from edwp.mem_prov_asgnmt_hist
                    	where substr(MEM_BUS_KEY_NUM, 1, 3) != 'MEM'
                    ) as PPG_S
        	    ) as PPG
                on Ca.cin_no = PPG.cin_no
                where Ca.adm_dt >= PPG.EFF_DT 
                and (Ca.adm_dt < PPG.TERM_DT or PPG.TERM_DT is null)
            ) S
            where rownumber2 = 1

            union  

            --2) via ENROLLKEY and plandata

            select case_id, ppg, ppg_name, case when ppg is null then 3 else 2 end as priority
            from   
            (
                select 
                    ca.case_id
                    , row_number() over (partition by ca.case_id order by ek.lastupdate desc, ek.createdate desc) as rn
                    , ipa.ppg
                    , ipa.enty_prov_nm as ppg_name
                from nathalie.prjrea_step4_procedures as ca
                left join
                (
                    select carriermemid, effdate, termdate, ratecode, createdate, lastupdate, planid, eligibleorgid, enrollid
                    from plandata.enrollkeys 
                    where segtype = 'INT' and ratecode <> 'CMCWELL'
                ) ek
                on ca.cin_no = ek.carriermemid
                and ca.adm_dt >= ek.effdate
                and ca.adm_dt <= ek.termdate
                left join
                ( --concern: not tied to admit date? 
                    select
                        ppg1.enrollid
                        , ppg1.ppg
                        , v.enty_prov_nm
                    from
                    (
                        select distinct
                            mp.enrollid
                            , regexp_replace(ap.ppg_fullname, '.*\\([A-z]+-|-.*\\)', '') as ppg
                        from plandata.memberpcp mp 
                        inner join
                        plandata.enrollkeys ek3
                        on mp.enrollid = ek3.enrollid and mp.termdate = ek3.termdate
                        inner join
                        (  /*TK HERE YOU HAVE OPPORTUNITY TO BRING IN PCP*/
                            select 
                                aff.affiliationid as pcpaffiliationdid
                                , aff.affiliateid
                                , p1.provid as ppg_provid
                                , p1.fullname as ppg_fullname
                            from plandata.affiliation aff
                            inner join
                            plandata.provider p1
                            on aff.affiliateid = p1.provid
                        ) ap
                        on mp.affiliationid = ap.pcpaffiliationdid
                        where mp.pcptype = 'PCP'
                    ) ppg1
                    inner join
                    edwp.vw_ppg v 
                    on ppg1.ppg = v.prov_bus_key_num
                ) ipa
                on ek.enrollid = ipa.enrollid                
            ) S
            where rn = 1

        ) ppg_inner_1
        
    ) ppg_inner_2
    
    where rn = 1 
    
) ppg
on A.case_id = ppg.case_id
;
