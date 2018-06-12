/*Profile of demographic fields in HOAP.HOA
AU: Nathalie Blume
Project: Readmission
tmp delete this line
*/

--count
select count(*) 
from HOA.members
; 
--5,471,618
select count(distinct cin_no) 
from HOA.members
;
--5,324,750 --> includes non-current members. 
select count(distinct a.cin_no) --count(distinct a.cin_no)
from HOA.members a
left join HOA.mbr_enroll_seg b
on a.cin_no = b.cin_no
where b.disenroll_dt is null
;
--1,438,547 --> too few. TK ELYA: who is a current members? 



--gender
select gender, count(gender)
from HOA. members
group by gender
;
/*
	0
M	2550915
F	2920539
*/

--ethnicity

