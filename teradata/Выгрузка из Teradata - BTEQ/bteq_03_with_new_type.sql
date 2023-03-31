.SET ECHOREQ OFF;
.SET SESSION CHARSET "UTF8";
.SET FORMAT OFF;
.SET TITLEDASHES OFF;
.SET WIDTH 2000;

.EXPORT REPORT FILE="c:\temp\unload_03_with_new_type.txt", LIMIT=**, CLOSE;

with SRC as (
  select databasename as db_name
        ,tablename as tbl_name
        ,cast(cast(createtimestamp as timestamp format'YYYY-MM-DDBHH:MI:SS') as varchar(30)) as crt_ts
    from DBC.tablesv
   where 1=1
     and databasename = 'prd3_1_db_dmplacc'
)
select col01
  from TABLE(
         CSV(NEW VARIANT_TYPE(src.db_name, src.tbl_name, src.crt_ts), ';', '')
         RETURNS (col01 varchar(2000) CHARACTER SET UNICODE)
        ) as DTA
   order by 1;

.EXPORT RESET;

.LOGOFF; 

.EXIT;
