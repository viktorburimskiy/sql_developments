DECLARE @columns VARCHAR(4000)
DECLARE @query NVARCHAR(4000)

SELECT @columns =  COALESCE(@columns + ',[' + CAST(Code_N AS VARCHAR) + ']', '[' + CAST(Code_N AS VARCHAR)+ ']')
FROM
(
	SELECT *, 
		CASE 
			WHEN Code in (1101,1102,1201,1202,1203,1204,1205,1301,1306,1311,1312,1315,1317,1403,1409,1410,1412,1502,1503,2101,2102,2103,2104,2105,2107,2201,2202,2302,2303,2310,2312,2401,2403,2404,2405,2408) THEN 'N_'+Code 
			WHEN Code in (1307,1308,1309,1310,1313,1316,1318,1411,1501,1504,2106,2203,2204,2305,2306,2307,2311,2501) THEN 'S_'+Code 
			WHEN Code in (3101,3102,3201) THEN 'D_'+Code
			WHEN StopFact=1 THEN 'S_'+ Code
			WHEN Negative=1 THEN 'N_'+ Code
			ELSE Code 
		END Code_N
	FROM
	(
		SELECT *
		FROM [CRIFSme].[dbo].[DE_F_SME_DAL]
		UNION ALL
		SELECT *
		FROM [CRIFSme].[dbo].[DE_F_SME_DAAL]
		WHERE Code <> -1
	) t0
	WHERE Code <> -1
) t 
GROUP BY t.Code_N
ORDER BY Code_N
--PRINT @columns

SET @query = '
				SELECT *
				FROM 
					(
						SELECT	d.Application_ID as [Номер сделки], 
								IsNull(CONVERT(varchar,d.x10_app_date,104),'''') as [Дата сделки], 
								IsNull(CONVERT(varchar,def.NPL_Date,104),'''') as [Дата дефолта], 
								IIF(StopFact= 0,''Нет'',''Да'') as [Выявлен стоп-фактор (да / нет)],  
								IIF(Negative= 0,''Нет'',''Да'') as [Выявлена негативна информация (да / нет)], Code_N
						FROM DE_F_SME_ADATA as d
						LEFT JOIN 
							(
								SELECT
									Application_ID,
									[Диапазон выдачи],
									NPL_Date,
									ROW_NUMBER() OVER (PARTITION BY Application_ID ORDER BY [Диапазон выдачи] DESC, NPL_Date DESC, [УУ: PL/NPL] DESC) as nm
								FROM dbo.Report_Manager_Mapping
							) as def
						ON def.Application_ID = d.application_id and def.nm = 1
						LEFT JOIN 
							(
								SELECT Application_ID, SubjectID, Code_N,
									MAX(StopFact) OVER (PARTITION BY Application_ID) as StopFact,  
									MAX(Negative) OVER (PARTITION BY Application_ID) as Negative
								FROM
								(
									SELECT *, 
										CASE 
											WHEN Code in (1101,	1102,1201,1202,1203,1204,1205,1301,1306,1311,1312,1315,1317,1403,1409,1410,1412,1502,1503,2101,2102,2103,2104,2105,2107,2201,2202,2302,2303,2310,2312,2401,2403,2404,2405,2408) THEN ''N_''+Code 
											WHEN Code in (1307,1308,1309,1310,1313,1316,1318,1411,1501,1504,2106,2203,2204,2305,2306,2307,2311,2501) THEN ''S_''+Code 
											WHEN Code in (3101,3102,3201) THEN ''D_''+Code
											WHEN StopFact=1 THEN ''S_''+ Code
											WHEN Negative=1 THEN ''N_''+ Code
											ELSE Code 
										END Code_N
									FROM
										(
											SELECT *
											FROM [CRIFSme].[dbo].[DE_F_SME_DAL]
											UNION ALL
											SELECT *
											FROM [CRIFSme].[dbo].[DE_F_SME_DAAL]
											WHERE Code <> -1
										) t0
									WHERE Code <> -1
								) t
							) as t0 
						 on t0.Application_ID = d.x540_MP_app_id 
						WHERE t0.Application_ID IS NOT NULL
					) t 
				PIVOT	( COUNT(Code_N) FOR Code_N IN (' + @columns + ')
						) as PVT
				WHERE [Номер сделки] IS NOT NULL and [Номер сделки] <> -1
			'	
--PRINT @query
EXECUTE(@query)
