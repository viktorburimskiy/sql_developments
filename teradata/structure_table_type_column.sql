SELECT 
	Lower(databasename) AS DatabaseNName
	,Lower(tablename) AS TableName
	,Lower(columnname) AS ColumnName
	,ColumnLength
	,ColumnFormat
	,CASE ColumnType
		WHEN 'BF' THEN 'byte(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'BV' THEN 'varbyte(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'CF' THEN 'char(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'CV' THEN 'varchar(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'D ' THEN 'decimal(' || Trim(DecimalTotalDigits) || ',' || Trim(DecimalFractionalDigits) || ')'
		WHEN 'DA' THEN 'date'
		WHEN 'F ' THEN 'float'
		WHEN 'I1' THEN 'byteint'
		WHEN 'I2' THEN 'smallint'
		WHEN 'I8' THEN 'bigint'
		WHEN 'I ' THEN 'integer'
		WHEN 'AT' THEN 'time(' || Trim(DecimalFractionalDigits) || ')'
		WHEN 'TS' THEN 'timestamp(' || Trim(DecimalFractionalDigits) || ')'
		WHEN 'TZ' THEN 'time(' || Trim(DecimalFractionalDigits) || ')' || ' with time zone'
		WHEN 'SZ' THEN 'timestamp(' || Trim(DecimalFractionalDigits) || ')' || ' with time zone'
		WHEN 'YR' THEN 'interval year(' || Trim(DecimalTotalDigits) || ')'
		WHEN 'YM' THEN 'interval year(' || Trim(DecimalTotalDigits) || ')'      || ' to month'
		WHEN 'MO' THEN 'interval month(' || Trim(DecimalTotalDigits) || ')'
		WHEN 'DY' THEN 'interval day(' || Trim(DecimalTotalDigits) || ')'
		WHEN 'DH' THEN 'interval day(' || Trim(DecimalTotalDigits) || ')'      || ' to hour'
		WHEN 'DM' THEN 'interval day(' || Trim(DecimalTotalDigits) || ')'      || ' to minute'
		WHEN 'DS' THEN 'interval day(' || Trim(DecimalTotalDigits) || ')'|| ' to second('|| Trim(DecimalFractionalDigits) || ')'
		WHEN 'HR' THEN 'interval hour(' || Trim(DecimalTotalDigits) || ')'
		WHEN 'HM' THEN 'interval hour(' || Trim(DecimalTotalDigits) || ')'      || ' to minute'
		WHEN 'HS' THEN 'interval hour(' || Trim(DecimalTotalDigits) || ')'      || ' to second('|| Trim(DecimalFractionalDigits) || ')'
		WHEN 'MI' THEN 'interval minute(' || Trim(DecimalTotalDigits) || ')'
		WHEN 'MS' THEN 'interval minute(' || Trim(DecimalTotalDigits) || ')'      || ' to second('|| Trim(DecimalFractionalDigits) || ')'
		WHEN 'SC' THEN 'interval second(' || Trim(DecimalTotalDigits) || ',' || Trim(DecimalFractionalDigits) || ')'
		WHEN 'BO' THEN 'blob(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'CO' THEN 'clob(' || Trim(Cast(ColumnLength AS INTEGER)) || ')'
		WHEN 'PD' THEN 'period(date)'     
		WHEN 'PM' THEN 'period(timestamp('|| Trim(DecimalFractionalDigits) || ')' || ' with time zone'
		WHEN 'PS' THEN 'period(timestamp('|| Trim(DecimalFractionalDigits) || '))'
		WHEN 'PT' THEN 'period(time(' || Trim(DecimalFractionalDigits) || '))'
		WHEN 'PZ' THEN 'period(time(' || Trim(DecimalFractionalDigits) || '))' || ' with time zone'
		WHEN 'UT' THEN Coalesce(ColumnUDTName,  '<Unknown> ' || ColumnType)
		WHEN '++' THEN 'td_anytype'
		WHEN 'N'  THEN 'number('|| CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE Trim(DecimalTotalDigits) END
								|| CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || Trim(DecimalFractionalDigits) END || ')'
		WHEN 'A1' THEN Coalesce('sysudtlib.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
		WHEN 'AN' THEN Coalesce('sysudtlib.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
		ELSE ColumnType
	END Type_nm
FROM dbc.columnsV
WHERE  Upper(DatabaseNName) = Upper('tst')
	AND Upper(tablename) LIKE Upper('%suo%')
	AND Upper(ColumnName) = Upper('SUBBRANCH_ID')
ORDER BY columnname





SELECT 
	tbl.DatabaseName
	,Lower(Cast(tbl.TABLENAME AS VARCHAR(100))) AS "TableName"
	,tbl.CreatorName
FROM dbc.tablesv tbl
WHERE 1 = 1
	AND Upper(tbl.databasename) = Upper('001_MIS_RETAIL_CHANNEL')   
	AND Upper(tbl.tablename) LIKE Upper('%SUBBRANCH%')	
	AND tbl.Tablekind IN ('T','O')
ORDER BY TableName DESC


--SUBBRANCH_ID
