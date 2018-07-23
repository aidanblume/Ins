/*
EDW data pool claims and encounters. 

Exploration on 20180321.
*/





/*S_MEM_CL
HOPE:
educational level

*/

select *
from EDW.S_MEM_CL
where rownum < 10
;
-- seems to be the same as EDW.EDWBTI.F_MEM_CL






/*VW_DIAG_CD
HOPE:
*/

select *
from EDW.VW_DIAG_CD
where rownum < 10
;
--just a ref table to match icd9 code to its descriptore






/*VW_HOH
HOPE:
*/

select *
from EDW.VW_HOH
where rownum < 10
;
--

select count(distinct joinkey_hoh)
from EDW.VW_HOH
;
--