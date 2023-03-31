create or replace function tech.sp_aux_recover_dependencies(i_sql_exec text) 
	returns void
	language plpgsql SECURITY DEFINER
as $$
	

	/**
	 * Description:
	 * 	Выполнение функции sql от имени создателя (security definer).
	 * 	На вход передается sql.
	 * 	
	 * Input:
	 * 	i_sql_exec - sql на выполнение
	 */
begin
	
	-- выполнение sql
	execute i_sql_exec;

end;

$$;
