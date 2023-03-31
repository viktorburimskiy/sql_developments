.SET ECHOREQ OFF;
.SET SESSION CHARSET "UTF8";
.SET FORMAT OFF;
.SET WIDTH 2000;
.SET SEPARATOR ";";
.SET TITLEDASHES OFF;

.EXPORT REPORT FILE="c:\temp\unload_01_with_spaces.txt", LIMIT=**, CLOSE;

select databasename (title 'db_name')
      ,tablename (title 'tbl_name')
      ,cast(cast(createtimestamp as timestamp format'YYYY-MM-DDBHH:MI:SS') as varchar(30)) (title 'crt_ts')
  from DBC.tablesv
 where 1=1
   and databasename = 'prd3_1_db_dmplacc'
 order by 1,2;

.EXPORT RESET;

.LOGOFF; 

.EXIT;
