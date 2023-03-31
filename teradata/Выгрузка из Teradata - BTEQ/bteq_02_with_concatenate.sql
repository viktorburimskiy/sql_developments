.SET ECHOREQ OFF;
.SET SESSION CHARSET "UTF8";
.SET FORMAT OFF;
.SET WIDTH 2000;
.SET SEPARATOR ";";
.SET TITLEDASHES OFF;

.EXPORT REPORT FILE="c:\temp\unload_02_with_concatenate.txt", LIMIT=**, CLOSE;

select coalesce(databasename, '') 
         || ';' || coalesce(tablename, '')
         || ';' || coalesce(cast(cast(createtimestamp as timestamp format'YYYY-MM-DDBHH:MI:SS') as varchar(30)), '') (title 'csv_data')
  from DBC.tablesv
 where 1=1
   and databasename = 'prd3_1_db_dmplacc'
 order by 1;

.EXPORT RESET;

.LOGOFF; 

.EXIT;
