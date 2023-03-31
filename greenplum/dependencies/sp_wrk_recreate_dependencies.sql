create or replace function tech.sp_wrk_recreate_dependencies(i_debug_flg boolean, i_workflow_id bigint, i_id bigint) 
	returns void
	language plpgsql
	volatile
as $body$
	
	/*
	 *	Description:
	 * 		функция развертывания зависимостей (вью).
	 * 		на вход передается oid таблицы для, которой ранее были сохранены зависимые вью. в зависимости от наименования схемы вью выполняется польз. функция в рамках этой схемы.
	 * 	
	 * 	Input:
	 * 		i_debug_flag - флаг дебага
	 * 		i_id - oid сохраненной ods таблицы
	 */

declare 
	l_command_nm text default 'create';
	l_tech_schema_nm text default 'tech'; -- название технической схемы
	l_recreate_proc_nm text default 'sp_aux_recreate_dependencies'; -- название процедуры по пересозданию объектов

	l_curr_row record;	-- строка ddl view для схемы
	l_delete_sql_query text default ''; -- запрос с удалением записей из tech_save_dependencies
	l_sql_exec text;	-- формирование строки вызова функции для развертывания ddl

begin
	
	l_delete_sql_query := 'delete from tech.tech_save_dependencies where table_id = ' || i_id;

	-- восстанавливаем объекты

	for l_curr_row in 
		select 
			t1.schema_name
			,t1.view_name
			,t1.view_owner
			,t1.dep_level
			,t1.type_sql
			,t1.sql_text
			,t2.grant_arr
		from tech.tech_save_dependencies t1
		join (select 
				view_id
				,json_agg(case when type_sql = 'grant' then sql_text end) grant_arr
			from tech.tech_save_dependencies
			where type_sql = 'grant'
			group by view_id) t2
			on t1.view_id = t2.view_id
		where t1.type_sql != 'grant'
			and t1.table_id = i_id
		order by t1.dep_level

	loop 
		-- выполняем развертывание запроса в схемах , вызовом польз. функции

		l_sql_exec := 'select ' || l_tech_schema_nm || '.' || l_recreate_proc_nm || '(' || i_debug_flg || ', ''' || l_curr_row.schema_name || ''',''' || l_curr_row.view_name || ''',''' || l_curr_row.view_owner || ''',''' || l_curr_row.type_sql || ''',''' || l_curr_row.sql_text || ''',''' || l_curr_row.grant_arr || ''',''' ||  l_command_nm || ''')';

		perform tech.sp_aux_recreate_dependencies(i_debug_flg, i_workflow_id, l_curr_row.schema_name, l_curr_row.view_name, l_curr_row.view_owner, l_curr_row.type_sql, l_curr_row.sql_text, l_curr_row.grant_arr, l_command_nm); 
	
		perform tech.sp_wrk_add_log(i_debug_flg, i_workflow_id, 'sp_wrk_recreate_dependencies', 'ods_table_oid = ' || i_id, l_sql_exec, clock_timestamp(), clock_timestamp(), 0);
	
	end loop;

	-- удаляем обработанные строки по таблице
	perform tech.sp_wrk_execute(i_debug_flg, l_delete_sql_query, i_workflow_id);

exception when others then 
	perform tech.sp_wrk_add_log(i_debug_flg,i_workflow_id,'sp_wrk_recreate_dependencies', 'ods_table_oid = ' || i_id, 'exception throw: ' || sqlstate || ' ' || sqlerrm, clock_timestamp(), clock_timestamp(), 0);

end;
$body$
execute on master;

