SELECT sqltext.TEXT,
req.session_id,
req.status,
s.login_name,
req.command,
req.cpu_time,
req.total_elapsed_time,
req.last_wait_type,
req.lock_timeout,
req.wait_time,
s.*
FROM sys.dm_exec_requests req
CROSS APPLY sys.dm_exec_sql_text(sql_handle) AS sqltext
LEFT JOIN sys.dm_exec_sessions s on req.session_id = s.session_id
where sqltext.TEXT not like '%req.session_id,
req.status,
s.login_name,
req.command,
req.cpu_time,
req.total_elapsed_time,
req.last_wait_type,	
req.lock_timeout,
req.wait_time%'
order by req.cpu_time desc 