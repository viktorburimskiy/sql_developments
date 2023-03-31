create or replace function tech.sys_audit_objects_table_text_return (p_schema_name text[])
	returns table (object_id oid, schema_name text, object_name text, arguments text, owner_name text, type_object text, description text, source_code text, creator_name text, create_dttm timestamp, modified_name text, modified_dttm timestamp)
	language plpgsql
	volatile
as $$

	/**
	 * Description:
	 * 	Формирование ddl табличных объектов
	 * 	select * from tst.sys_audit_objects_table_text_return (ARRAY['tst', 'tech'])
	 *	select * from tst.sys_audit_objects_table_text_return ('{tst, tech}')
	 * 	
	 * Input:
	 * 	p_schema_name - список схем
	 *
	 */

begin
	
	--raise notice 'array - %', p_schema_name;
	
	return query 
		select 
			c.oid as object_id
			,cast(n.nspname as text) as schema_name
			,n.nspname || '.' || c.relname as object_name
			,null::text as arguments
			,cast(pg_get_userbyid(c.relowner) as text) as owner_name
			,case when c.relstorage = 'x' then 'x' else 't' end as type_object
			,obj_description(c.oid) as description
			,concat(
					'CREATE ', case when c.relstorage = 'x' then	--тип таблицы
									case when pe."writable" 
										then 'WRITABLE EXTERNAL ' 
										else 'READABLE EXTERNAL ' 
									end
								end,  'TABLE '
					,n.nspname || '.' || c.relname	--схема и имя таблицы
					,' (', chr(10), cl.full_text_column, chr(10) || ')'	--столбцы
					,case when array_length(c.reloptions, 1) > 0 then chr(10) || 'WITH (' || array_to_string(c.reloptions, ', ') || ')' end	--атрибуты секции with
					,case when c.relstorage = 'x' --атрибуты external table
						then chr(10) || 'LOCATION (''' || array_to_string(pe.urilocation, ',') || ''') on ' || case when array_to_string(pe.execlocation, ',') = 'ALL_SEGMENTS' then 'ALL' else array_to_string(pe.execlocation, ',') end ||
							 chr(10) || 'FORMAT ''' || case pe.fmttype 
							 								when 't' then 'TEXT' || ''' (' || pe.fmtopts || ')'
							 								when 'c' then 'CSV' || ''' (' || pe.fmtopts || ')'
							 								when 'b' then 'CUSTOM' || ''' (' || regexp_replace(pe.fmtopts, '\s', '=') || ')'
							 							end || chr(10) || 'ENCODING ''' || pg_encoding_to_char(pe."encoding") || ''''
					end 
					
					,coalesce(chr(10) || pg_get_table_distributedby(c.oid), '') --дистрибюция
					,coalesce(chr(10) || pg_get_partition_def(c.oid, true), '') -- партиции
					,coalesce(chr(10) || 'comment on table ' || n.nspname || '.' || c.relname || ' is ''' || obj_description(c.oid) || '''', '')	--описание таблицы
					,case when cl.full_text_comment is not null then chr(10) || cl.full_text_comment end	--описание столбцов
					) as source_code
			,cast(cr_oper.stausename as text) as creator_name
			,cr_oper.statime::timestamp as create_dttm
			,cast(al_oper.stausename as text) as modified_name
			,al_oper.statime::timestamp as modified_dttm
		from pg_catalog.pg_class c
		left join pg_catalog.pg_namespace n 
			on n.oid = c.relnamespace
		--столбцы таблицы и описание
		left join 	(select 
						t.id
						,string_agg(full_text_column, ',' || chr(10)) as full_text_column
						,string_agg(full_text_comment, chr(10)) as full_text_comment
					from (	select 
								pc.oid as id
								--ddl columns
								,concat (
										chr(9)
										,a.attname
										,' '
										,format_type(a.atttypid, a.atttypmod)
										,case when a.attnotnull then ' not null' else ' null' end
										,coalesce(' DEFAULT ' || pg_get_expr(ad.adbin, pc.oid), '') ) as full_text_column
								--ddl comment for columns
								,case when pd.description is not null 
									then concat (
												'comment on column '
												,pc.oid::regclass
												,'.'
												,a.attname
												,' is '''
												,pd.description
												,''';' ) 
								end as full_text_comment
								,row_number () over (partition by pc.oid order by a.attnum) as rn	--для сортировки столбцов в порядке их создания
							from pg_catalog.pg_class pc
							join pg_catalog.pg_namespace np
								on pc.relnamespace = np.oid
							left join pg_catalog.pg_attribute a
								on a.attrelid = pc.oid 
								and a.attnum > 0 
								and not a.attisdropped
							left join pg_catalog.pg_attrdef ad
								on ad.adrelid = a.attrelid
								and ad.adnum = a.attnum
								and a.atthasdef
							left join pg_catalog.pg_description as pd
								on a.attnum = pd.objsubid
								and pd.objoid = pc.oid 
							where np.nspname = any(p_schema_name)  -- т.к. при выборе всех вылетает ошибка доступа к другим схемам
							) as t
					group by t.id ) as cl
			on cl.id = c.oid
		--create table
		left join pg_catalog.pg_stat_last_operation as cr_oper
			on cr_oper.objid = c.oid
			and cr_oper.staactionname = 'CREATE'
		--alter table
		left join (	select 
						objid
						,stausename
						,statime
						,row_number() over (partition by objid order by statime desc) as rn
					from pg_catalog.pg_stat_last_operation
					where stausename != 'gpadmin'
						and  staactionname in ('ALTER', 'PARTITION')) as al_oper
			on al_oper.objid = c.oid
			and al_oper.rn = 1
		--убираем партиции из перечня таблиц
		left join (	select 
						coalesce (t1.inhparent, t0.inhparent) inhparent
						,coalesce (t0.inhrelid ,t1.inhrelid) inhrelid
					from pg_catalog.pg_inherits as t0
					left join pg_catalog.pg_inherits as t1
						on t0.inhparent = t1.inhrelid) prt
			on prt.inhrelid = c.oid
		--инфа по external table
		left join pg_catalog.pg_exttable pe 
			on pe.reloid = c.oid
		where 1=1
			and n.nspname =  any(p_schema_name)
			and c.relkind = 'r'
			and inhrelid is null;

end;
$$
execute on master;create or replace function tech.sys_audit_objects_table_text_return (p_schema_name text[])
	returns table (object_id oid, schema_name text, object_name text, arguments text, owner_name text, type_object text, description text, source_code text, creator_name text, create_dttm timestamp, modified_name text, modified_dttm timestamp)
	language plpgsql
	volatile
as $$

	/**
	 * Description:
	 * 	Формирование ddl табличных объектов
	 * 	select * from tst.sys_audit_objects_table_text_return (ARRAY['tst', 'tech'])
	 *	select * from tst.sys_audit_objects_table_text_return ('{tst, tech}')
	 * 	
	 * Input:
	 * 	p_schema_name - список схем
	 *
	 * Creator:
	 *	Буримский В.А.
	 */

begin
	
	--raise notice 'array - %', p_schema_name;
	
	return query 
		select 
			c.oid as object_id
			,cast(n.nspname as text) as schema_name
			,n.nspname || '.' || c.relname as object_name
			,null::text as arguments
			,cast(pg_get_userbyid(c.relowner) as text) as owner_name
			,case when c.relstorage = 'x' then 'x' else 't' end as type_object
			,obj_description(c.oid) as description
			,concat(
					'CREATE ', case when c.relstorage = 'x' then	--тип таблицы
									case when pe."writable" 
										then 'WRITABLE EXTERNAL ' 
										else 'READABLE EXTERNAL ' 
									end
								end,  'TABLE '
					,n.nspname || '.' || c.relname	--схема и имя таблицы
					,' (', chr(10), cl.full_text_column, chr(10) || ')'	--столбцы
					,case when array_length(c.reloptions, 1) > 0 then chr(10) || 'WITH (' || array_to_string(c.reloptions, ', ') || ')' end	--атрибуты секции with
					,case when c.relstorage = 'x' --атрибуты external table
						then chr(10) || 'LOCATION (''' || array_to_string(pe.urilocation, ',') || ''') on ' || case when array_to_string(pe.execlocation, ',') = 'ALL_SEGMENTS' then 'ALL' else array_to_string(pe.execlocation, ',') end ||
							 chr(10) || 'FORMAT ''' || case pe.fmttype 
							 								when 't' then 'TEXT' || ''' (' || pe.fmtopts || ')'
							 								when 'c' then 'CSV' || ''' (' || pe.fmtopts || ')'
							 								when 'b' then 'CUSTOM' || ''' (' || regexp_replace(pe.fmtopts, '\s', '=') || ')'
							 							end || chr(10) || 'ENCODING ''' || pg_encoding_to_char(pe."encoding") || ''''
					end 
					
					,coalesce(chr(10) || pg_get_table_distributedby(c.oid), '') --дистрибюция
					,coalesce(chr(10) || pg_get_partition_def(c.oid, true), '') -- партиции
					,coalesce(chr(10) || 'comment on table ' || n.nspname || '.' || c.relname || ' is ''' || obj_description(c.oid) || '''', '')	--описание таблицы
					,case when cl.full_text_comment is not null then chr(10) || cl.full_text_comment end	--описание столбцов
					) as source_code
			,cast(cr_oper.stausename as text) as creator_name
			,cr_oper.statime::timestamp as create_dttm
			,cast(al_oper.stausename as text) as modified_name
			,al_oper.statime::timestamp as modified_dttm
		from pg_catalog.pg_class c
		left join pg_catalog.pg_namespace n 
			on n.oid = c.relnamespace
		--столбцы таблицы и описание
		left join 	(select 
						t.id
						,string_agg(full_text_column, ',' || chr(10)) as full_text_column
						,string_agg(full_text_comment, chr(10)) as full_text_comment
					from (	select 
								pc.oid as id
								--ddl columns
								,concat (
										chr(9)
										,a.attname
										,' '
										,format_type(a.atttypid, a.atttypmod)
										,case when a.attnotnull then ' not null' else ' null' end
										,coalesce(' DEFAULT ' || pg_get_expr(ad.adbin, pc.oid), '') ) as full_text_column
								--ddl comment for columns
								,case when pd.description is not null 
									then concat (
												'comment on column '
												,pc.oid::regclass
												,'.'
												,a.attname
												,' is '''
												,pd.description
												,''';' ) 
								end as full_text_comment
								,row_number () over (partition by pc.oid order by a.attnum) as rn	--для сортировки столбцов в порядке их создания
							from pg_catalog.pg_class pc
							join pg_catalog.pg_namespace np
								on pc.relnamespace = np.oid
							left join pg_catalog.pg_attribute a
								on a.attrelid = pc.oid 
								and a.attnum > 0 
								and not a.attisdropped
							left join pg_catalog.pg_attrdef ad
								on ad.adrelid = a.attrelid
								and ad.adnum = a.attnum
								and a.atthasdef
							left join pg_catalog.pg_description as pd
								on a.attnum = pd.objsubid
								and pd.objoid = pc.oid 
							where np.nspname = any(p_schema_name)  -- т.к. при выборе всех вылетает ошибка доступа к другим схемам
							) as t
					group by t.id ) as cl
			on cl.id = c.oid
		--create table
		left join pg_catalog.pg_stat_last_operation as cr_oper
			on cr_oper.objid = c.oid
			and cr_oper.staactionname = 'CREATE'
		--alter table
		left join (	select 
						objid
						,stausename
						,statime
						,row_number() over (partition by objid order by statime desc) as rn
					from pg_catalog.pg_stat_last_operation
					where stausename != 'gpadmin'
						and  staactionname in ('ALTER', 'PARTITION')) as al_oper
			on al_oper.objid = c.oid
			and al_oper.rn = 1
		--убираем партиции из перечня таблиц
		left join (	select 
						coalesce (t1.inhparent, t0.inhparent) inhparent
						,coalesce (t0.inhrelid ,t1.inhrelid) inhrelid
					from pg_catalog.pg_inherits as t0
					left join pg_catalog.pg_inherits as t1
						on t0.inhparent = t1.inhrelid) prt
			on prt.inhrelid = c.oid
		--инфа по external table
		left join pg_catalog.pg_exttable pe 
			on pe.reloid = c.oid
		where 1=1
			and n.nspname =  any(p_schema_name)
			and c.relkind = 'r'
			and inhrelid is null;

end;
$$
execute on master;
