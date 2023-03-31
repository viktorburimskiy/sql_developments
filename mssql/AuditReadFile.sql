USE [AUDIT]
GO

/****** Object:  View [dbo].[AuditReadFile]    Script Date: 10/8/2021 5:29:50 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







CREATE VIEW [dbo].[AuditReadFile]
as

	select *
	from (
		select 
			f.session_id
			,f.object_id
			,'Audit' as "type_id"
			,case 
				when f.action_id in ('LGO', 'LO') then 'A1'
				when f.action_id in ('LGIS', 'LGSD') then 'A2'
				when f.action_id in ('LGIF') then 'A3'
				when f.action_id = 'CR' and tp.class_type_desc in ('SQL USER', 'SQL LOGIN', 'WINDOWS LOGIN', 'WINDOWS USER', 'USER') then 'B1'
				when f.action_id = 'G' then 'B3'
				when f.action_id in ('AL','LGDB','LGLG') and tp.class_type_desc in ('SQL LOGIN', 'SQL USER') then 'B5'
				when f.action_id = 'DR' and tp.class_type_desc in ('SQL USER', 'SQL LOGIN') then 'B7'
				when f.action_id in ('PWR', 'PWEX', 'PWPL')  and tp.class_type_desc = 'SQL LOGIN' then 'B9'
				when f.action_id = 'CR' and tp.class_type_desc = 'ROLE' then 'B11'
				when f.action_id = 'DR' and tp.class_type_desc = 'ROLE' then 'B13'
				when f.action_id in ('DPRL','APRL','R') and tp.class_type_desc in ('ROLE','SERVER ROLE','SERVER') then 'B0'
				when f.action_id in ('AL', 'AUSC') and tp.class_type_desc in ('SERVER AUDIT SPECIFICATION', 'SERVER AUDIT') then 'D3'
				when f.action_id = 'DR  ' and tp.class_type_desc = 'SERVER AUDIT' then 'D1'
			end as subtype_id
			,f.client_ip end as ip_address
			,f.server_principal_name as "user_login"
			,case 
				when act.name = 'LOGIN SUCCEEDED' then 'SUCCESS' 
				when act.name = 'LOGIN FAILED' then 'FAIL' 
				when act.name = 'LOGOUT' then 'SUCCESS'
				else 'SUCCESS'
			end as "status"
			,cast(f.event_time as datetime) as operation_date
			,case when f.action_id = 'LGIF' then lower(f.[statement]) end as reason
			,f.server_principal_name as editor_user_login
			,rol.role_name as editor_role
			,case 
				when f.action_id = 'G' then 
					case when len(f.target_server_principal_name) > 0 then f.target_server_principal_name else f.target_database_principal_name end
				when f.action_id in ('AL','LGDB','LGLG') and tp.class_type_desc in ('SQL LOGIN', 'SQL USER') then f.object_name 
				when f.action_id = 'DR' and tp.class_type_desc in ('SQL USER', 'SQL LOGIN') then f.object_name
			end as edited_user_login
			,case when f.action_id in ('CR') and tp.class_type_desc in ('SQL USER', 'SQL LOGIN', 'WINDOWS LOGIN', 'WINDOWS USER', 'USER', 'ROLE') then f.object_name end as created_role
			,case when f.action_id = 'DR' and tp.class_type_desc = 'ROLE' then f.object_name end as edited_role
			,'' as params
			,lower(f.[statement]) as "sql_text"
			,f.transaction_id
			,'Microsoft SQL Server' as system_name
			,f.application_name
		from sys.fn_get_audit_file('D:\AUDIT_SQL\*.sqlaudit', default, default) as f
		join (select distinct 
				action_id
				, name
			from sys.dm_audit_actions) act
			on act.action_id = f.action_id
		join sys.dm_audit_class_type_map tp
			on tp.class_type = f.class_type
		left join (
			select
				coalesce(l.name, u.name) us_name
				,case when (r.principal_id is null) then 'public' else r.name end role_name
			from sys.database_principals u
				left join (sys.database_role_members m join sys.database_principals r on m.role_principal_id = r.principal_id) on u.principal_id = m.member_principal_id
				left join sys.server_principals l on u.sid = l.sid
			where u.type <> 'R') as rol
			on rol.us_name = f.server_principal_name
		where 1=1
			and f.server_principal_name <> 'NT SERVICE\SQLTELEMETRY'
			and object_id >= 0
			--and tp.class_type_desc in ('SQL USER','ROLE','WINDOWS LOGIN','WINDOWS USER', 'SERVER','SERVER ROLE')
		--order by operation_date desc
	) t
	where t.subtype_id is not null
		and len(t.sql_text) > 1 
	--where subtype_id in ('A1','A2','A3','B1','B3','B5','B7','B9')

	union all

	select 
		t.session_id
		,t.[object_id]
		,t.[type_id]
		,case 
			when t.sql_text like '%RoleID%' and t.sql_text like '%DatabasePermission%' then 'B5'
			when t.sql_text like '%RoleID%' then 'B11'
			when t.params = 'AuditLogin' then 'A2'
			when t.params = 'AuditLogout' then 'A1'
			when t.sql_text like 'SELECT%' then 'C7'
		end as subtype_id
		,t.ip_address
		,t.user_login
		,case 
			when t.Success = 1 and t.Error = 0 then 'SUCCESS'
			when t.Error = 1 then 'FAIL'
			else ''
		end as [status]
		,t.operation_date
		,t.reason
		,'' as editor_user_login
		,'' as editor_role
		,'' as edited_user_login
		,'' as created_role
		,'' as edited_role
		,t.params
		,t.sql_text
		,t.transaction_id
		,'Microsoft Analysis Server' as system_name
		,t.application_name
	from
		(select
			a.event_data
			,/*coalesce(a.event_data.value('(event/data[@name="SessionID"]/value)[1]', 'varchar(150)'), '')*/ null as session_id
			,null as [object_id]
			,'Audit' as type_id
			,coalesce(a.event_data.value('(event/data[@name="ClientHostName"]/value)[1]', 'varchar(50)'), '') as ip_address
			,a.event_data.value('(event/data[@name="NTCanonicalUserName"]/value)[1]', 'varchar(200)') as user_login
			,a.event_data.value('(event/data[@name="Success"]/value)[1]', 'varchar(10)') as Success
			,a.event_data.value('(event/data[@name="Error"]/value)[1]', 'varchar(10)') as Error
			,a.event_data.value('(event/@timestamp)[1]', 'datetime') as operation_date
			,'' as reason
			,a.object_name as params
			,coalesce(a.event_data.value('(event/data[@name="TextData"]/value)[1]', 'varchar(max)'), '') as sql_text
			,'' as transaction_id
			,a.event_data.value('(event/data[@name="ServerName"]/value)[1]', 'varchar(50)') as ServerName
			,a.event_data.value('(event/data[@name="DatabaseName"]/value)[1]', 'varchar(200)') as DatabaseName
			,a.event_data.value('(event/data[@name="ApplicationName"]/value)[1]', 'varchar(200)') as application_name 
			,cast(a.event_data.value('(event/data[@name="RequestProperties"]/value)[1]', 'varchar(max)') as XML) as RequestProperties
		from (select 
				object_name
				,timestamp_utc
				,cast(event_data as XML) event_data
			 from sys.fn_xe_file_target_read_file('D:\AUDIT_SSAS\*.xel', null, null, null) ) a
		) t
	where 1=1
		and t.params in ('AuditLogin','AuditLogout','QueryEnd','CommandEnd')



GO


