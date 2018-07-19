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
PREP
*/

/*
APRDRG descriptions
*/
select 
--create table A (var 1 string, var 2 string) row format delimited fields terminated by ',' stored as Parquet location '/user/nathalieb/aprdrg_codes.csv';
create table A (var aprdrg string, var description string) row format delimited fields terminated by ',' stored as Parquet location '/user/nathalieb/aprdrg_codes.csv';

/*
CASES BY LOB
*/
drop table if exists nathalie.LOB;
create table nathalie.LOB (input string, output string);
--select * from nathalie.LOB limit 40;

insert into nathalie.LOB
values (
(NULL,	NULL)
,('10',	'MCLA')
,('10MCE',	'MCLA-MCE')
,('10CCI',	'MCLA-CCI')
,('10TANF',	'MCLA-TANF')
,('10SPD',	'MCLA-SPD')
,('20',	'Healthy Family Plan')
,('30',	'PASC-SEIU')
,('40',	'PASC-SEIU')
,('60',	'Healthy Kids')
,('70',	'Dual Eligible Special Needs Plan')
,('80',	'Cal-Medi Connect (CMC)')
,('90',	'LA Care Covered/Health Benefits Exchange')
,('-1',	'Other')
);

select * from nathalie.lob;

/*
CREATE DATA SET
*/

drop table if exists nathalie.njb_descriptive_set
;
create table nathalie.njb_descriptive_set
as
select
    A.*
    , B.output as lob
from
(
    select 
        cin_no
        , agegp_hedis
        , aprdrg
        , concat(product_code, COALESCE(segment, '')) as lob_input
        , is_a_30d_readmit
        , paid_amt_case
    from NATHALIE.NJB_ANALYTIC_SET 
    where adm_dt between '2017-01-01' and '2017-12-31'
    --and is_a_30d_death != 1
) as A
left join nathalie.lob as B
on A.lob_input = B.input
;

/*

/*
COUNT MEMBERS
*/
select count(distinct cin_no) from nathalie.njb_descriptive_set;
--84793 incl death
--83471 excl death

/*
COUNT id_a_30d_readmit by BY LOB

PRINT RESULT BELOW
*/

select 
    lob
    , sum(is_a_30d_readmit) as Readmit_Count
    , count(*) as Total_Count
    , round(sum(is_a_30d_readmit)/count(*)*100, 2) as Readmit_Rate
    , round(sum(case when is_a_30d_readmit=1 then paid_amt_case else 0 end),2) as Paid_Amt_Readmit_Claims
    --, sum(paid_amt_case) as Paid_Amount
from nathalie.njb_descriptive_set
group by lob
order by sum(is_a_30d_readmit) desc
;


/*
CASES BY APRDRG [Keep top 20]

PRINT RESULT BELOW
*/
select 
    aprdrg
    , B.description
    , sum(is_a_30d_readmit) as Readmit_Count
    , count(*) as Total_Count
    , round(sum(is_a_30d_readmit)/count(*)*100, 2) as Readmit_Rate
    , round(sum(case when is_a_30d_readmit=1 then paid_amt_case else 0 end),2) as Paid_Amt_Readmit_Claims
from nathalie.njb_descriptive_set as A
left join nathalie.aprdrg_codes_csv as B
on A.aprdrg = B.apr_drg
group by A.aprdrg, B.description
order by sum(is_a_30d_readmit) desc
;


select * from aprdrg_codes_csv limit 10;
/*
CASES BY AGE GROUP

PRINT RESULT BELOW
*/
select 
    Age_Group
    , Readmit_Count
    , Total_Count
    , Readmit_Rate
    , Paid_Amt_Readmit_Claims
from 
(
    select 
        agegp_hedis
        , case
            when agegp_hedis = 'C' then '1-child' 
            when agegp_hedis = 'A' then '2-adult'
            when agegp_hedis = 'O' then '3-older adult'
            else null
        end as Age_Group
        , sum(is_a_30d_readmit) as Readmit_Count
        , count(*) as Total_Count
        , round(sum(is_a_30d_readmit)/count(*)*100, 2) as Readmit_Rate
        , round(sum(case when is_a_30d_readmit=1 then paid_amt_case else 0 end),2) as Paid_Amt_Readmit_Claims
    from nathalie.njb_descriptive_set
    group by agegp_hedis
) as S
order by Age_Group
;

/*
CASES BY APRDRG, LOB

PRINT RESULT BELOW
*/

select 
    lob, aprdrg
    , sum(is_a_30d_readmit) as Readmit_Count
    , count(*) as Total_Count
    , round(sum(is_a_30d_readmit)/count(*)*100, 2) as Readmit_Rate
    , round(sum(case when is_a_30d_readmit=1 then paid_amt_case else 0 end),2) as Paid_Amt_Readmit_Claims
    --, sum(paid_amt_case) as Paid_Amount
from nathalie.njb_descriptive_set
group by lob, aprdrg
order by sum(is_a_30d_readmit) desc
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
from nathalie.njb_descriptive_set
--where adm_age > 17 --TK filter is needed, but data field is poorly populated and reduces counts too much. Need to find alternative DOB source. 
--and adm_age < 65
--where is_followed_by_a_30d_death = 0
group by product_code, segment, is_followed_by_a_30d_readmit
order by product_code, segment, is_followed_by_a_30d_readmit
;

