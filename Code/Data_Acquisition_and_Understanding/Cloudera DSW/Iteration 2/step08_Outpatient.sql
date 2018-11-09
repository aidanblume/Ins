/***
Title:              step8_Outpatient
Description:        Will eventually be a post-discharge outpatient services step
Version Control:    https://dsghe.lacare.org/nblume/Readmissions/tree/master/Code/Data_Acquisition_and_Understanding/Cloudera%20DSW/Iteration2/
Data Source:        prjrea_step7_hospitals
Output:             nathalie.prjrea_step8_Outpatient
***/

/*
PASS THROUGH
*/

drop table if exists nathalie.prjrea_step8_Outpatient
;

create table nathalie.prjrea_step8_Outpatient
as
select *
from nathalie.prjrea_step7_hospitals as A
;
