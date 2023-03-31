
Перейти к концу метаданных
Создал(а) Филиппов Иван Евгеньевич дек 02, 2020
Переход к началу метаданных
/*Возвращает состояние кеша запросов. Позволяет посмотреть что происходило на сервере в ближайшее время, так как запросы в кеше постоянно замещаются. Первыми выводятся самые долго исполняемые по времени. */

/*Average Duration + Query Text + Query Plan*/

use master;
go

select top 100
(total_elapsed_time/execution_count)/1000000 as [Duration],
(total_worker_time/execution_count)/1000000 as [CPU Time],
(select text from sys.dm_exec_sql_text(sql_handle)) as [Query Text],
(select query_plan from sys.dm_exec_query_plan(plan_handle)) as [Query Plan],
execution_count,
(total_physical_reads + total_logical_writes + total_logical_reads) as [Total IO]
from sys.dm_exec_query_stats order by [Duration] desc;
go

/* Смотрим что выполняется на сервере в данный момент. Для быстрого ответа на вопрос: "что делает сервер" */

/*Info Requests*/
select
session_id,
blocking_session_id,
status,
command,
(select text from sys.dm_exec_sql_text(sql_handle)) as [Query Text],
(select query_plan from sys.dm_exec_query_plan(plan_handle)) as [Query Plan]
from sys.dm_exec_requests where session_id > 50;


/* Получаем информацию о блокировках, первый запрос выводит головные блокировки, от которых появляются остальные. Второй запрос выводит общее колличество блокировок. Чтобы представить масштаб бедствия */

/* Blocking session + object name */ /*Head blocing*/
select distinct blocking_session_id from sys.dm_exec_requests where blocking_session_id <> 0;
select count(distinct session_id) as [Wait tasks] from sys.dm_os_waiting_tasks where session_id > 50;

/* Запрос выводит более подробную информацию о запросах, блокировках и таблицах к которым идет обращение. */

/*Info blocing*/
declare @requests varchar(750);
declare @db varchar(50) = 'DM_FAST';
declare @id varchar(10);

set @id = db_id(@DB);

select @requests = 'select top 100
session_id,
blocking_session_id,
wait_type,
status,
command,
db_name(database_id) as [DB name],
(select name from '+ @db +'.sys.objects where object_id = resource_associated_entity_id) as [Object Name],
(select text from sys.dm_exec_sql_text(sql_handle)) as [Query Text],
(select query_plan from sys.dm_exec_query_plan(plan_handle)) as [Query Plan]
from sys.dm_exec_requests join sys.dm_tran_locks on request_session_id = session_id
where resource_type = OBJECTand resource_database_id = '+ @id +' order by wait_time desc;'
exec (@requests);


/* Запрос выводит потерянные распределенные транзакции. В колонке request_owner_guid выводится идентификатор транзакции. Он нужен для завершения потерянной распределенной транзакции. */ /* Distributed transaction */

select db_name(resource_database_id) as [database],
request_session_id,
resource_type,
request_owner_guid
from sys.dm_tran_locks where request_session_id = -2 and resource_type = 'OBJECT';

/* Вместо request_owner_guid вставляется номер из этой колонки для завершения потерянной распределенной транзакции. */ /*request_owner_guid*/

kill 'request_owner_guid';


/* sys.dm_exec_sessions - отображает сведения обо всех активных соединениях пользователя и внутренних задачах, sys.dm_exec_requests - сведения о каждом из запросов, выполняющихся в SQL Server, sys.dm_tran_locks - сведения о ресурсах диспетчера блокировок, активного в данный момент, sys.dm_os_waiting_tasks - сведения об очереди задач, о жидающих освобождения определенного ресурса. */

-- Requiest status --
select
n.session_id as [SID],
db_name(n.database_id) as [Database],
n.blocking_session_id as [Blocking SID],
n.wait_type,
i.original_login_name as [Login],
n.total_elapsed_time as [Duration],
i.host_name as [Host name],
i.client_interface_name,
i.status,
cast(i.login_time as smalldatetime) as [Login Time],
i.program_name as [Program],
cast(n.start_time as smalldatetime) as [Start Request],
n.wait_time,
n.cpu_time,
n.wait_resource,
n.status as [Status],
user_name(n.user_id) as [User],
n.open_transaction_count,
n.percent_complete,
n.task_address,
n.command as [Command],
s.text as [Request Text]
from sys.dm_exec_sessions as i join sys.dm_exec_requests as n on i.session_id = n.session_id
cross apply sys.dm_exec_sql_text(n.sql_handle) as s where n.session_id > 50

-- Blocking status --
select n.session_id as [SID],
db_name(i.resource_database_id) as [Database],
n.blocking_session_id as [Blocking SID],
n.wait_type,
i.resource_type,
i.request_status,
i.request_owner_type,
i.lock_owner_address,
n.resource_address
from sys.dm_tran_locks as i join sys.dm_os_waiting_tasks as n on i.lock_owner_address = n.resource_address
order by n.session_id asc

--Селект на количество планов исполнения запросов в кэше--

SELECT objtype, count(*) FROM sys.dm_exec_cached_plans

GROUP BY objtype

--CPU Utilization History for last 256 minutes--

DECLARE @ts_now bigint = (SELECT cpu_ticks/(cpu_ticks/ms_ticks)FROM sys.dm_os_sys_info);

SELECT TOP(256) SQLProcessUtilization AS [SQL Server Process CPU Utilization],

SystemIdle AS [System Idle Process],

100 - SystemIdle - SQLProcessUtilization AS [Other Process CPU Utilization],

DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) AS [Event Time]

FROM (

SELECT record.value('(./Record/@id)[1]', 'int') AS record_id,

record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')

AS [SystemIdle],

record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]',

'int')

AS [SQLProcessUtilization], [timestamp]

FROM (

SELECT [timestamp], CONVERT(xml, record) AS [record]

FROM sys.dm_os_ring_buffers WITH (NOLOCK)

WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'

AND record LIKE N'%<SystemHealth>%') AS x

) AS y

ORDER BY record_id DESC OPTION (RECOMPILE);


/--Информация по свободному месту на LUN, где базы

SELECT DB_NAME(f.database_id) AS [DatabaseName], f.file_id,

vs.volume_mount_point, vs.total_bytes/1024/1024/1024 as 'Всего,Гб', vs.available_bytes/1024/1024 as 'Cвободно,Гб',

CAST(CAST(vs.available_bytes AS FLOAT)/ CAST(vs.total_bytes AS FLOAT) AS DECIMAL(18,3)) * 100 AS [Space Free %]

FROM sys.master_files AS f WITH (NOLOCK) --where servername='srb-itreport02'

CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs

ORDER BY f.database_id OPTION (RECOMPILE);

/--Считает, сколько процессоров не хватает для нормальной работы

SELECT ROUND(((CONVERT(FLOAT, ws.wait_time_ms) / ws.waiting_tasks_count) / si.os_quantum

* scheduler_count),2) AS Additional_Sockets_Necessary

FROM sys.dm_os_wait_stats ws

CROSS APPLY sys.dm_os_sys_info si

WHERE ws.wait_type = 'SOS_SCHEDULER_YIELD'


----------------------------------------------------------

Посмотреть, что находится в бекапе (в одинарных кавычках - полный путь к файлу):

restore filelistonly from disk = 'L:\backup.sql\analits_3098863_2011-06-03.bak'

PS: Если файлов копии несколько, указываем их через запятую. disk = 'L:\backup.sql\analits_3098863_2011-06-03-1.bak', disk = 'L:\backup.sql\analits_3098863_2011-06-03-2.bak' ... итд