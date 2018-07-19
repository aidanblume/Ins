            select cin_no, adm_dt, dis_dt, snf_90dfwd_tmp, days_until_SNF_tmp, adm_dt_SNF, dis_dt_SNF, SNF 
                -- , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_SNF_tmp asc, dis_dt_SNF desc) as rownumber -- keep the earliest and longest valid stay for any SNF
                , row_number() over(partition by cin_no, adm_dt, dis_dt order by days_until_SNF_tmp desc, dis_dt_SNF desc) as rownumber -- keep the LATER (and that being equal, the longest) valid stay for any SNF. Reason: Responsibiity for readmission lies with the last SNF to have custody of the member. 
            from 
            ( -- Select SNF Cases that were active during the 90 day pre-inpatient admission window
                select IP.cin_no
                    , IP.adm_dt
                    , IP.dis_dt
                    , SNF.adm_dt as adm_dt_SNF
                    , SNF.dis_dt as dis_dt_SNF
                    , SNF.provider as SNF
                    , case 
                            when datediff(SNF.adm_dt, IP.dis_dt) < 0 then 0
                            else datediff(SNF.adm_dt, IP.dis_dt)
                        end as days_until_SNF_tmp
                    , 1 as snf_90dfwd_tmp
                from prjrea_step4d_SNF as IP
                left join 
                (-- Select unique SNF cases (did not look for contiguous ones)
                    select distinct case_id, cin_no, adm_dt, dis_dt, provider
                    from
                    ( --add number rows inside partitions where each partition is a unique (cin_no, admi_dt, dis_dt) tupple
                        select case_id, cin_no, adm_dt, dis_dt, provider, source_table
                        , row_number() over(partition by cin_no, adm_dt, dis_dt order by source_table asc, case_id desc) as rownumber
                        from
                        ( -- union of cases across 3 data tables: qnxt, clm, enc
                            select case_id, cin_no, adm_dt, dis_dt, provider
                            , 1 as source_table
                            from hoap.QNXT_CASE_INPSNF
                            where srv_cat = '04snf'
                            union
                            select case_id, cin_no, adm_dt, dis_dt, provider
                            , 2 as source_table
                            from hoap.clm_case_inpsnf
                            where srv_cat = '04snf'
                            union
                            select case_id, cin_no, adm_dt, dis_dt, provider
                            , 3 as source_table
                            from hoap.ENC_CASE_INPSNF
                            where srv_cat = '04snf'
                       ) AS ALL_CASES
                    ) ALL_CASES_PARTITIONED
                ) as SNF
                on IP.cin_no = SNF.cin_no
                where days_add(IP.dis_dt, IP.days_until_next_admit) >= SNF.adm_dt --keep SNF that started before the next IP admit (eliminate SNF that began after next IP admit)
                and IP.dis_dt < SNF.dis_dt --keep SNF that existed after IP discharge (eliminate SNF stays that ended before IP discharge)
            ) X
  