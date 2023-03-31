create or replace function tech.sp_aux_recreate_dependencies(i_debug_flg boolean, i_workflow_id bigint, i_schema_nm text, i_view_nm text, i_owner_nm text, i_type_nm text, i_ddl_sql text, i_dcl_sql json, i_command_nm text) 
	returns void
	language plpgsql
	volatile
	security definer
	
as $$

	/*
	 *	Description:
	 * 		Выполнение функции sql от имени создателя (security definer).
	 * 		На вход передается sql.
	 * 	
	 * 	Input:
	 * 		i_schema_nm - схема
	 * 		i_view_nm - наименование вью
	 * 		i_owner_nm - владелец объекта
	 * 		i_type_nm - тип ddl (view/matview/grant)
	 * 		i_ddl_sql - ddl views
	 * 		i_dcl_sql - ddl grants
	 * 		i_command_nm - тип команды
	 */ 

declare 

	l_dcl_sql text default '';
	l_create_sql_query text default ''; -- запрос на создание
	l_drop_sql_query text default ''; -- запрос на удаление
	l_alter_sql_query text default '';

begin
	
	if i_command_nm = 'create' then

		--меняем владельца
		l_alter_sql_query = 'alter table ' || i_schema_nm || '.' || i_view_nm || ' owner to ' || i_owner_nm || ';';
		
		--собираем гранты
		select string_agg('grant ' || dcl_sql || ';', chr(10)) dcl_sql 
		into l_dcl_sql
		from json_array_elements_text(i_dcl_sql) as dcl (dcl_sql);
	
	
		--собираем итоговую команду
		l_create_sql_query := 'create ' || i_type_nm || ' ' || i_schema_nm || '.' || i_view_nm || ' as ' || replace(i_ddl_sql, ';', '') || ';' || chr(10)
								|| l_alter_sql_query || chr(10)
								|| l_dcl_sql;
								
		if (i_debug_flg) then 
			raise notice 'create_sql_query - %', l_create_sql_query;
		else 
			execute l_create_sql_query;
		end if;
		

	elsif i_command_nm = 'drop' then

		l_drop_sql_query := 'drop ' || i_type_nm || ' ' || i_schema_nm || '.' || i_view_nm || ';';
		
		if (i_debug_flg) then 
			raise notice 'drop_sql_query - %', l_drop_sql_query;
		else 
			execute l_drop_sql_query;
		end if;
		
	end if;

exception when others then 
	raise exception 'exception throw: %', sqlstate || ' ' || sqlerrm;

end;

$$
execute on master;

