SELECT execquery.last_execution_time AS [Date Time], execsql.text AS [Script] FROM sys.dm_exec_query_stats AS execquery
CROSS APPLY sys.dm_exec_sql_text(execquery.sql_handle) AS execsql
ORDER BY execquery.last_execution_time DESC

/*
или
*/

SELECT cp.objtype AS PlanType,
       OBJECT_NAME(st.objectid,st.dbid) AS ObjectName,
       cp.refcounts AS ReferenceCounts,
       cp.usecounts AS UseCounts,
       st.text AS SQLBatch,
       qp.query_plan AS QueryPlan
FROM sys.dm_exec_cached_plans AS cp
CROSS APPLY sys.dm_exec_query_plan(cp.plan_handle) AS qp
CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st;