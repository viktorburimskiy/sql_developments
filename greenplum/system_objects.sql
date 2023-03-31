/*********************************************************************************************
Объем
*********************************************************************************************/
--размер БД
select pg_size_pretty(pg_database_size(current_database()))
	,pg_database_size(current_database())/1024^3 as size_gb
	,pg_database_size(current_database())/(1024*1024) as size_gb
	,pg_database_size(current_database())/1024 as size_kb
	,pg_database_size(current_database()) as size_b
	
--размер таблицы на диске
select pg_size_pretty(pg_relation_size('pcap_sn_data_layer.us_smsb1_data_state_log_buf'));

--размер таблицы+индексы на диске
select pg_size_pretty(pg_total_relation_size('pcap_sn_data_layer.us_smsb1_data_state_log_buf'))

--сколько дискового пространства использует таблица (после недавнего применения VACUUM или ANALYZE - т.к. знаечние relpages обновляеться после этих команд (1 стр = 8kb))
select relpages, pg_relation_filepath(oid)
from pg_catalog.pg_class
where relname='us_smsb1_data_state_log_buf'
/*********************************************************************************************
SKEW
*********************************************************************************************/
--распределение данных таблицы по сегментам
select gp_segment_id, count(*) cnt_row
from pcap_sn_data_layer.us_smsb1_data_state_log_buf
group by gp_segment_id



/*********************************************************************************************
Исходный код и описание объекта
*********************************************************************************************/
--процедура/функция
select 
	p.oid as proc_id
	,proname as proc_name
	,pn.nspname as proc_schema_name
	,us.usename as create_user
	,prosrc as sql_code
	,lang.lanname as language_name
	,case p.provolatile
		when 'i' then 'immutable'
		when 's' then 'stable'
		when 'v' then 'volatile'
	end as proc_properti_name
	,p.pronargs as count_arg
	,p.proargnames as proc_arg_name
	,p.proargtypes as proc_arg_type
	,p.prorettype as prco_resoult_type_id
	,pt.typname as prco_resoult_type_name
from pg_catalog.pg_proc as p
--схема
inner join pg_catalog.pg_namespace as pn
	on pn.oid = p.pronamespace
--юзеры
inner join pg_catalog.pg_user as us
	on us.usesysid = p.proowner
--язык
inner join pg_catalog.pg_language as lang
	on lang.oid = p.prolang
--тип
inner join pg_catalog.pg_type as pt
	on pt.oid = p.prorettype
where proname in ('us_p_smsb_data_load', 'vsp_p_isu_etl_set_log_err')

--или
select oid , pg_get_functiondef(oid)
from pg_catalog.pg_proc pp 
where proowner = 199057


--представления
select
	viewname as view_name
	,schemaname as view_schema_name
	,viewowner as create_user
	,definition as sql_code
from pg_catalog.pg_views pv
where viewname = 'v_mytable'
--или
select pg_get_viewdef('pcap_sn_data_layer.v_mytable')
/*********************************************************************************************
Статистика
*********************************************************************************************/
--размер и статистика использования индекса
select 
	t.schemaname as schema_name
	,t.tablename as table_name
	,index_name
	,pc.reltuples as cnt_rows
	,pg_size_pretty(pg_relation_size(t.schemaname || '.' || t.tablename)) as table_size
	,pg_size_pretty(pg_relation_size(t.schemaname || '.' || sq.indexrelname)) as index_size
	,case when sq.indisunique
		then 1
		else 0
	end as is_unique_idx
	,sq.idx_scan
	,sq.idx_tup_read
	,sq.idx_tup_fetch
from pg_catalog.pg_tables as t
left join pg_catalog.pg_class as pc
	on t.tablename = pc.relname
--индекс
left join
	(
		select
			pc.relname as c_table_name
			,pc1.relname as index_name
			,pi.indnatts as number_of_column
			,psai.idx_scan
			,psai.idx_tup_read
			,psai.idx_tup_fetch
			,psai.indexrelname
			,pi.indisunique
		from pg_catalog.pg_index as pi
		inner join pg_catalog.pg_class as pc
			on pc.oid = pi.indrelid
		inner join pg_catalog.pg_class as pc1
			on pc1.oid = pi.indexrelid
		inner join pg_catalog.pg_stat_all_indexes as psai
			on psai.indexrelid = pi.indexrelid
	) as sq
	on sq.c_table_name = t.tablename
where 1=1
	--and t.tablename = 'us_smsb1_data_state_log_buf'
	and t.schemaname in ('pcap_sn_us', 'pcap_sn_suo', 'pcap_sn_isu_rb', 'pcap_sn_data_layer')

--инфа по сжатию таблицы (данные есть, если в пользовательской таблице присутствую эти опции (секция WITH) )
select 
	relid as table_id
	,blocksize
	,safefswritesize
	,compresslevel
	,checksum
	,compresstype
	,columnstore
from  pg_catalog.pg_appendonly
where relid = 379309

--если сжатие таблицы было по столбцам (см pg_catalog.pg_appendonly | columnstore = True)
select *
from pg_catalog.pg_attribute_encoding


--когда последний раз выполнялся vacuum и analyze
select
	pn.nspname
	,pc.relname
	,pslo.staactionname
	,pslo.stasubtype
	,pslo.statime
	,pc.relacl
	,pc.reloptions
	,pn.nspacl
from pg_catalog.pg_class as pc
inner join pg_catalog.pg_namespace as pn
	on pn.oid = pc.relnamespace
left join pg_catalog.pg_stat_last_operation as pslo
	on pslo.objid = pc.oid
		and pslo.staactionname in ('VACUUM','ANALYZE')
where 1=1
	and pc.relkind in ('r', 's')
	and pc.relstorage in ('h','a','c')
	and pn.nspname in ('pcap_sn_us', 'pcap_sn_suo', 'pcap_sn_isu_rb', 'pcap_sn_data_layer')
order by pn.nspname, pc.relname

-- статистика таблицы (стиатистика на основе данных таблицы)
select
	schemaname
	,tablename
	,attname
	,inherited
	,null_frac
	,avg_width as "Средний размер строки"
	,n_distinct
	,most_common_vals
	,most_common_freqs
	,histogram_bounds
	,correlation as "Упорядочность значений "
	,most_common_elems
	,most_common_elem_freqs
	,elem_count_histogram
from pg_catalog.pg_stats
where tablename = 'us_smsb1_data_state_log_day_buf'
/*********************************************************************************************
Профиль юзера и ресурсные группы
*********************************************************************************************/
--профиль пользователя
select
	rol.rolname as role_name --user_name
	,rol.rolsuper
	,rol.rolinherit
	,rol.rolcreaterole
	,rol.rolcreatedb
	,rol.rolcatupdate
	,rol.rolcanlogin
	,rol.rolreplication
	,rol.rolconnlimit
	,rol.rolvaliduntil
	,rol.rolresqueue as resource_group_id
	,rol.oid as role_id --user id
	,rol.rolcreaterextgpfd
	,rol.rolcreaterexthttp
	,rol.rolcreatewextgpfd
	,rol.rolresgroup as res_group_id
	,res.rsgname as res_group_name
	,res.parent as res_group_parent
from pg_catalog.pg_roles as rol
inner join pg_catalog.pg_resgroup as res
	on rol.rolresgroup = res.oid
where oid = 199057;

--ресурсные группы
select
	pr.oid as res_group_id
	,pr.rsgname
	,pr.parent
	,prc.resgroupid
	,prc.reslimittype as limit_type_id
	,case prc.reslimittype
		when 0 then 'Unknown'
		when 1 then 'Concurrency'
		when 2 then 'CPU'
		when 3 then 'Memory'
		when 4 then 'Memory shared quota'
		when 5 then 'Memory spill ratio'
		when 6 then 'Memory auditor'
		when 7 then 'CPU set'
	end as limit_type_name
	,prc.value as limit_value
from pg_catalog.pg_resgroup as pr
inner join pg_catalog.pg_resgroupcapability as prc
	on prc.resgroupid = pr.oid

-- группа пользователя. и все учестники этой группы
select * 
from pg_catalog.pg_group 
where (select usesysid from pg_catalog.pg_user where usename = 'burimskiy_va') = ANY(grolist);
/*********************************************************************************************
Права доступа
*********************************************************************************************/
select 
	pu.usesysid as user_id
	,pu.usename as user_name
	,pn.nspname as schema_name
	,pc.relname as object_name
	,pc.relkind as type_object_code
	,case pc.relkind
		when 'r' then 'обычная таблица'
		when 'i' then 'индекс'
		when 'S' then 'последовательность'
		when 'v' then 'вью'
		when 'm' then 'материал. вью'
		when 'c' then 'состовной тип'
		when 't' then 'таблица TOAST'
		when 'f' then 'сторонняя таблица'
	end as type_object_name
	,pu2.usename as user_owner_name
	,(pu2.usename != pu.usename and pc.relacl::text !~ ('({|,)' || pu.usename || '=')) as public
	,pc.relacl as rules_user_on_object
from pg_catalog.pg_user as pu
cross join pg_catalog.pg_class as pc
left join pg_catalog.pg_namespace as pn
	on pn.oid = pc.relnamespace
left join pg_catalog.pg_user as pu2
	on pu2.usesysid = pc.relowner
where 1=1
	and (pc.relowner = pu.usesysid
		or pc.relacl::text ~ ('({|,)(|' || pu.usename || ')='))
	and (pu2.usename != pu.usename and pc.relacl::text !~ ('({|,)' || pu.usename || '=')) = false --убираем общедоступные объекты
	and pu.usesysid = 199057
	and pc.relkind not in ('t','i','S')
	
order by schema_name, object_name
