select count(*)
from CKOLTP.HRA
;
select *
from CKOLTP.HRA
;
select *
from CKOLTP.hra_category
;
select *
from CKOLTP.hra_task
;
select *
from CKOLTP.org_cm_case_history
;

--THE FOLLOWING HAS FINAL SCORES on 27-dec-2013 only (so does not contain current production data)
select *
from CKOLTP.m_member_hra
;
select event_date
from CKOLTP.m_member_hra
where rownum = 1
order by event_date desc
;
