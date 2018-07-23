--I need a data dictionary or some boxed queries 
--what are commonly updated tables?
--is thissame as QMEIS?

SELECT *
FROM CKOLTP.DIARY_ENTRY 
WHERE ROWNUM <10
; -- 1 ROW.EMPTY

SELECT *
FROM CKOLTP."_2_6_agent_bak"
WHERE ROWNUM <10
; -- EMPTY

SELECT *
FROM CKOLTP.fda_drug_code
WHERE ROWNUM <10
; -- full

SELECT *
FROM CKOLTP.hra_category
WHERE ROWNUM <10
; -- full

SELECT *
FROM CKOLTP.hra
WHERE ROWNUM <10
; -- full

SELECT *
FROM CKOLTP.member_triage
WHERE ROWNUM <10
; -- full

SELECT *
FROM CKOLTP.SQL_SERVER_VS_ORACLE_ROWS
WHERE ROWNUM <10
; -- EMPTY

SELECT *
FROM CKOLTP.um_case_action
WHERE ROWNUM <10
order by complete_date desc
; -- ONLY HAS STUFF UP TO 2014


