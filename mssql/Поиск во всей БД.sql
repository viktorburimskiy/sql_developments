--Поиск текстового значения по всем таблицам БД
DECLARE @table_name VarChar(255)
DECLARE @column_name VarChar(255)
DECLARE @SQL varchar(255)
DECLARE @str VarChar(255)
CREATE TABLE #t([value] int)
--Итоговая таблица ркзультата
CREATE TABLE #rez(TABLE_NAME VarChar(255),COLUMN_NAME VarChar(255))
---------------------------------------------------------------------------------------------------
SET @str='Заработная плата'	--Это значение мы и ищим, собственно...
---------------------------------------------------------------------------------------------------
--Курсор для просмотра таблиц
DECLARE cursor_rowguid CURSOR FOR
		SELECT isc.TABLE_NAME, isc.COLUMN_NAME 
		FROM INFORMATION_SCHEMA.COLUMNS isc
		INNER JOIN information_schema.tables ist ON isc.table_name = ist.table_name
		WHERE Table_Type = 'BASE TABLE' and DATA_TYPE LIKE '%char%' and isc.TABLE_NAME LIKE 'DE%' and isc.TABLE_NAME NOT LIKE '%tmp' 
		ORDER BY TABLE_NAME
OPEN cursor_rowguid
FETCH NEXT FROM cursor_rowguid
INTO @table_name,@column_name
WHILE @@FETCH_STATUS = 0
BEGIN	
	BEGIN TRY		
		SET @SQL='SELECT COUNT(' + @column_name + ') FROM ' + @table_name + ' WHERE ' + @column_name + ' LIKE ''%' + @str + '%'''		
		INSERT INTO #t exec(@SQL)		
		IF(SELECT TOP 1 [value] FROM #t)>0 INSERT INTO #rez(TABLE_NAME,COLUMN_NAME) VALUES (@table_name,@column_name)		
		DELETE #t
	END TRY
	BEGIN CATCH
		PRINT @table_name
	END CATCH
	FETCH NEXT FROM cursor_rowguid
	INTO @table_name,@column_name
END
CLOSE cursor_rowguid
DEALLOCATE cursor_rowguid
DROP TABLE #t
SELECT TABLE_NAME AS 'ИМЯ ТАБЛИЦЫ',COLUMN_NAME AS 'ИМЯ КОЛОНКИ' FROM #rez
--DROP TABLE #rez