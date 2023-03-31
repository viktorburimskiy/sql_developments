REPLACE MACRO TAB_SIZE(db_name VARCHAR(50), creator_name VARCHAR(100), table_name VARCHAR(100)) AS (
--REPLACE MACRO TAB_SIZE AS (
	SELECT 
		tbl.DatabaseName
		,cast(tbl.TABLENAME as varchar(100)) AS "TableName"
		,tbl.CreatorName
		,MIN(tbl.CreateTimeStamp) AS CreateTimeStamp
		,MAX(tbl.LastAccessTimeStamp) AS LastAccessTimeStamp
		,SUM(tsize.currentperm) / 1024**3 AS PermSizeGb
		,MAX(tsize.currentperm) / AVG(tsize.currentperm) (DECIMAL (18, 2)) AS SkewRatio
		,MAX(tsize.currentperm/1024**3) AS BytesUsedTopAMPGb
		,MIN(tbl.CreateTimeStamp) AS CreateTimeStamp
		,MIN(tbl.LastAlterTimeStamp) AS LastAlterTimeStamp
	FROM dbc.tablesv tbl
	JOIN dbc.tablesizev tsize
		ON tbl.TABLENAME = tsize.TABLENAME
			AND tbl.Databasename = tsize.Databasename
	    	AND tbl.Tablekind IN ('T','O')
	WHERE 1 = 1
		AND UPPER(tbl.databasename) = UPPER(:db_name)   
		AND (UPPER(tbl.creatorname)=UPPER(:creator_name) OR UPPER(:creator_name)='NULL')
		AND (UPPER(tbl.tablename)=UPPER(:table_name) OR UPPER(:table_name)='NULL')	   
	GROUP BY 1, 2, 3
	ORDER BY PermSizeGb DESC
	;		

);

--EXEC TAB_SIZE;

--EXEC TAB_SIZE('001_mis_retail_channel_lab','Yatskov-MS','usr_my_us_hw_fails');

--EXEC TAB_SIZE('001_mis_retail_channel_lab','NULL','usr_my_us_hw_fails');

EXEC TAB_SIZE('001_mis_retail_channel_lab','Kustov-AS','null');

