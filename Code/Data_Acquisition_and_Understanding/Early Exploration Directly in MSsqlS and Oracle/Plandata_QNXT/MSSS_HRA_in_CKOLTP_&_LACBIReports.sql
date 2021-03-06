/****** Script for SelectTopNRows command from SSMS  ******/
SELECT top 10 *
  FROM [LACBIREPORTS].[dbo].[APS_HRA_DETAIL_REPORT]
  ;

  /****** Script for SelectTopNRows command from SSMS  ******/
SELECT distinct question_value
  FROM [LACBIREPORTS].[dbo].[APS_HRA_DETAIL_REPORT]
  ;

SELECT count(distinct member_id)
  FROM [LACBIREPORTS].[dbo].[APS_HRA_DETAIL_REPORT]
  ;


--HRA scores per Arun Sekar
/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP 1000 [CID]
      ,[concept_id]
      ,[STR_VALUE]
      ,[unit_id]
      ,[update_time]
      ,[obx_id]
      ,[NUM_VALUE]
      ,[date_value]
      ,[concept_type_id]
      ,[TREND]
      ,[SYS_DATE]
      ,[user_name]
      ,[source_guid]
      ,[db_rowversion]
  FROM [CSNLACSQL06_CKOLTP].[dbo].[P_MEMBER_CONCEPT_VALUE]

  select * 
  from [CSNLACSQL06_CKOLTP].[dbo].[P_MEMBER_CONCEPT_VALUE] 
  where CONCEPT_id =504732; -- and cid = 'CCA Member Primary Key';
