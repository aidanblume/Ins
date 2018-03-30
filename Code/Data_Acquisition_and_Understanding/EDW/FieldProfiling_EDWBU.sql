/*
EDW data pool claims and encounters. 

Exploration on 20180321.
*/





/*MEM_DEMO_HIST
HOPE:
*/

select *
from EDWBTI.MEM_DEMO_HIST
where rownum < 10
;
--

select count(distinct joinkey_mem)
from EDWBTI.MEM_DEMO_HIST
where (zip is not null)
;
--