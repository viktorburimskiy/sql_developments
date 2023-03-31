/*SELECT  @@Servername AS Server ,
        DB_NAME() AS DBName ,
        isc.Table_Name AS TableName ,
        isc.Table_Schema AS SchemaName ,
        Ordinal_Position AS  Ord ,
        Column_Name ,
        Data_Type ,
        Numeric_Precision AS  Prec ,
        Numeric_Scale AS  Scale ,
        Character_Maximum_Length AS LEN , -- -1 means MAX like Varchar(MAX) 
        Is_Nullable ,
        Table_Type
FROM     INFORMATION_SCHEMA.COLUMNS isc
        INNER JOIN  information_schema.tables ist
              ON isc.table_name = ist.table_name 
      WHERE Table_Type = 'BASE TABLE' -- 'Base Table' or 'View' 
ORDER BY DBName ,
        TableName ,
        SchemaName ,
        Ordinal_position*/

--SELECT C.TABLE_NAME, C.COLUMN_NAME, CAST(ex.value AS VARCHAR(1000)) [Description]
--FROM INFORMATION_SCHEMA.COLUMNS C
--LEFT JOIN sys.extended_properties ex
--ON  ex.major_id=OBJECT_ID(C.TABLE_NAME,'U')
--AND ex.minor_id=COLUMNPROPERTY(OBJECT_ID(C.TABLE_NAME,'U'),C.COLUMN_NAME,'ColumnId')
--ORDER BY C.TABLE_NAME, C.ORDINAL_POSITION


--описание таблиц
SELECT objname As [TABLE_NAME], Value As [Description] FROM ::fn_listextendedproperty(null, 'user', 'dbo', 'table', NULL, NULL, NULL)

--описание таблиц и полей
select object_name(c.object_id) as table_name, c.name as col_name, l.value, t.Value
from sys.columns c 
   join sys.objects o 
      on c.object_id = o.object_id
   cross apply 
     fn_listextendedproperty (NULL, 'schema', schema_name(o.schema_id), 'table', object_name(c.object_id), 'column', c.name) l
left join 
	fn_listextendedproperty(null, 'user', 'dbo', 'table', NULL, NULL, NULL) t on object_name(c.object_id)=t.objname
           
   
/*
SELECT DISTINCT TABLE_NAME
FROM     INFORMATION_SCHEMA.COLUMNS isc
*/


