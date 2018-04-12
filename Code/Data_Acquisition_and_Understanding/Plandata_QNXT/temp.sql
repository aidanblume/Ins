WHILE (select count(distinct claimid_set) from #MyTable2) > 0
BEGIN

	SELECT row_number() over (order by memid asc, startdate asc, enddate asc) as row#
	, claimid
	, memid
	, startdate
	, enddate 
	, totalpaid
	, billclasscode 
	, billclass_desc
	, type_of_service
	, admittype 
	, admit_type_desc
	, Emergency2
	, LOB
	, provid
	, providername
	, npi
	, PCP_Provid
	, PCP
	, SEC_Provid
	, SEC_PCP
	, Member_PPGH
	, Primarydiag
	, ICDVersion
	, dob
	, sex
	into #temp
	from #MyTable
	;
	





	SELECT row_number() over (order by memid asc, startdate asc, enddate asc) as row#
	, claimid_set
	, memid
	, startdate
	, enddate
	INTO #MyTable3
	FROM #MyTable2	order by row#
	;


	SELECT A_row#, A_memid, B_memid, A_claimid_set, B_claimid_set, A_startdate, A_enddate, B_startdate, B_enddate, interval, concat(A_claimid_set, ', ', B_claimid_set) as claimid_set,
	CASE
		WHEN interval < 2 THEN 1
		ELSE 0
	END AS contiguous
	into #MyTable4
	FROM
	(
		SELECT 
		A.row# as A_row#, A.memid as A_memid, B.memid as B_memid, A.claimid_set as A_claimid_set, B.claimid_set as B_claimid_set
		, A.startdate as A_startdate, A.enddate as A_enddate, B.startdate as B_startdate, B.enddate as B_enddate, 
		CASE
			WHEN A.memid = B.memid THEN DATEDIFF(d, A. enddate, B.startdate)
				ELSE NULL
			END AS interval
		FROM #MyTable3 AS A INNER JOIN #MyTable3 AS B ON B.row# = A.row# + 1
	) AS S
	;


	insert into #FinalTable
	select *
	from #MyTable4
	where contiguous = 0
	;


	select *
	into #MyTable5
	from #MyTable4
	where contiguous = 1
	;


	select A_memid as memid, claimid_set as claimid, A_startdate as startdate
	, case 
		when A_enddate < B_enddate THEN B_enddate
		else A_enddate
		end as enddate
	, claimid_set
	into #MyTable6
	from #MyTable5
	;


	IF (sysdatetime() > '2018-04-10 10:55:00.00')
		BREAK;
	ELSE
		CONTINUE;

	--WAITFOR DELAY '00:00:02';
	drop table #MyTable2;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable3;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable4;
	--WAITFOR DELAY '00:00:02';
	drop table #MyTable5;
	WAITFOR DELAY '00:00:02';
	select *
	into #MyTable2
	from #MyTable6
	;

	drop table #MyTable6;
END