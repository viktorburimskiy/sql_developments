create or replace function tech.sp_wrk_save_dependencies(i_debug_flag boolean, i_workflow_run_id bigint, i_id bigint, i_column_list text, i_ods_schema_nm text) 
	returns void
	language plpgsql
	volatile
as $body$

	/**
	 * 	Description:
	 * 		Функция сохранения зависимостей (вью) в таблицу sys_save_related_view.
	 * 	
	 * 	Input:
	 * 		i_debug_flag - флаг дебага
	 *		i_workflow_run_id - id загрузки
	 * 		i_id - oid таблицы
	 *		i_column_list - список столбцов
	 *		i_ods_schema_nm - схема
	 *  
	 */
	
declare 	
	l_cnt int4 default 0;

	l_create_temp_table_sql_query text default '';
	l_drop_temp_table_sql_query text default '';
	l_insert_data_sql_query text default '';

	l_command_nm text default 'drop';
	l_curr_row record;	-- строка ddl view для схемы
	l_sql_exec text;

begin
	
	begin
		l_drop_temp_table_sql_query := 'drop table if exists dep_object';
	
		l_create_temp_table_sql_query := '
			create temporary table dep_object with (appendonly=false) as
			with recursive cte as 
			(
				select pc0.oid as id
					,pc0.relname table_nm
					,pc0.oid as parent_id
					,0 dep_level
				from pg_catalog.pg_class pc0 
				where pc0.oid = ' || i_id || '
				
				union  all
				
				select
					p1.id
					,cte.table_nm
					,cte.parent_id
					,cte.dep_level + 1
				from (
					select pc.oid as id
						,d.refobjid as parent_id
					from pg_catalog.pg_class pc 
					left join (
						select d.refobjid, r.ev_class 
						from pg_depend d 
						join pg_catalog.pg_rewrite r 
							on d.objid = r.oid
							and d.refobjsubid > 0
						group by d.refobjid, r.ev_class
						) d
						on pc.oid = d.ev_class 
					)p1
				join cte 
					on cte.id = p1.parent_id
			), clear as 
			(
				select id, parent_id, table_nm, dep_level, row_number() over (partition by id order by dep_level desc) rn 
				from cte 
			) 
				select id, parent_id, table_nm, dep_level
				from clear 
				where rn=1
					and id != parent_id
				distributed randomly;';
		
		l_insert_data_sql_query := 'with entity_info as (
				select 
					dep.id as view_id
					,dep.parent_id as table_id
					, dep.table_nm
					, dep.dep_level
					, relname
					, pn.nspname
					, relowner
					, relacl
					, relkind
				from dep_object dep
				join pg_catalog.pg_class pc
					on dep.id = pc.oid
				join pg_namespace pn 
					on pc.relnamespace = pn.oid
			)
			insert into tech.tech_save_dependencies (table_id, table_nm, view_id, dep_level, schema_name, view_name, sql_text, view_owner, type_sql)
			select 
				table_id
				,table_nm
				,view_id
				,dep_level
				,schema_name
				,view_name
				,sql_text
				,view_owner
				,type_sql
			from 
			(	
				select 
					pc.table_id
					,pc.table_nm
					,pc.view_id
					,pc.dep_level
					,coalesce(pv.schemaname, pm.schemaname) as schema_name
					,coalesce(pv.viewname, pm.matviewname) as view_name
					,lower(case when pc.dep_level=1 
								then ''select ' || i_column_list || ' from ' || i_ods_schema_nm || '.'' || pc.table_nm || '';'' 
								else coalesce(pv.definition, pm.definition) 
						end) as sql_text
					,coalesce(pv.viewowner, pm.matviewowner) as view_owner
					,case when lower(pc.relkind) = ''v''
						then ''view'' 
						else ''materialized view''
					end as type_sql
				from entity_info pc 
				left join pg_views pv 
					on pc.nspname = pv.schemaname
						and pc.relname = pv.viewname
				left join pg_matviews pm
					on pc.nspname = pm.schemaname
						and pc.relname = pm.matviewname
					
				union all
				
				--get grants
				select 
					pc.table_id
					,pc.table_nm
					,pc.view_id
					,pc.dep_level
					,pc.nspname as schema_name
					,pc.relname as view_name
					,concat(case when s[2] = ''arwdDxt'' 
							then ''all''
							else ''select''
						end 
						,'' on table ''
						,pc.view_id::regclass
						,'' to "''
						,lower(replace(coalesce(s[1], ''''), ''"'',''''))
						,''";''
					) as sql_text
					,replace(s[3], ''"'', '''') as view_owner
					,''grant'' as type_sql
				from entity_info pc
				join pg_catalog.pg_roles as pr
					on pr.oid = pc.relowner,
				unnest (coalesce(pc.relacl::text[], format(''{%s=arwdDxt/%s}'', pr.rolname, pr.rolname)::text[])) as acl,
				regexp_split_to_array(acl, ''=|/'') as s
				
			) t
			where not exists (select 1 from tech.tech_save_dependencies where view_id = t.view_id);';
		
		perform tech.sp_wrk_execute(i_debug_flag, l_drop_temp_table_sql_query, i_workflow_run_id);
		perform tech.sp_wrk_execute(i_debug_flag, l_create_temp_table_sql_query, i_workflow_run_id);
	
		if exists(select 1 from pg_class where relname = 'dep_object') or i_debug_flag then
			perform tech.sp_wrk_execute(i_debug_flag, l_insert_data_sql_query, i_workflow_run_id);
			perform tech.sp_wrk_execute(i_debug_flag, l_drop_temp_table_sql_query, i_workflow_run_id);
		end if;
	end;

	--удаляем объекты
	begin

		for l_curr_row in 
			select distinct 
				schema_name
				,view_name
				,type_sql 
				,dep_level
			from tech.tech_save_dependencies
			where type_sql != 'grant'
				and table_id = i_id
			order by dep_level  desc

		loop 
		
			l_sql_exec := 'drop ' || l_curr_row.type_sql || ' ' || l_curr_row.schema_name || '.' || l_curr_row.view_name;
			
			perform tech.sp_aux_recreate_dependencies(i_debug_flag, i_workflow_run_id, l_curr_row.schema_name, l_curr_row.view_name, null, l_curr_row.type_sql, null, null, l_command_nm); 
	
			perform tech.sp_wrk_add_log(i_debug_flag, i_workflow_run_id, 'sp_wrk_recreate_dependencies', 'ods_table_oid = ' || i_id, l_sql_exec, clock_timestamp(), clock_timestamp(), 0);
		
		end loop;
		
	end;

exception when others then 
	perform tech.sp_wrk_add_log(i_debug_flag, i_workflow_run_id, 'sp_wrk_save_dependencies', 'id = ' || i_id || e'\column_list = ' || i_column_list  || e'\ods_schema_nm = ' || i_ods_schema_nm, 'exception throw: ' || sqlstate || ' ' || sqlerrm, clock_timestamp(), clock_timestamp(), 0);
end;


$body$
execute on master;

