create or replace function tech.sp_wrk_add_log(i_debug_flg boolean, i_flow_id bigint, i_proc_name text, i_param text, i_sql_query text, i_run_dttm_st timestamp with time zone, i_run_dttm_end timestamp with time zone, i_str_count bigint) 
	returns void
	language plpgsql
as $$

	
	
	/*
	 * Description:
	 *	процедура sp_add_log добавляет логи в таблицу tech_logs
	 *
	 * Input:
	 * 	i_debug_flg - флаг дебага, в случае true - лог не записывается, но выводится информация в консоль
	 * 	i_flow_id - id потока, под которым происходит логирование
	 * 	i_proc_name - название процедуры, из которой дёргается данная
	 * 	i_param - параметры, используемые в данной операции
	 * 	i_sql_query - запрос, который выполнялся или статус старта/завершения процедуры, если таковых операций было много в ней
	 * 	i_run_dttm_st - время начала операции
	 * 	i_run_dttm_end - время окончания операции
	 * 	i_str_count - кол-во вставленных строк
	 */
		   
begin
	raise notice 'procedure sp_add_log starting';
	
	if (i_debug_flg) then 
		raise notice 'INSERT INTO tech.tech_logs % VALUES (%)', E'\n', i_flow_id || E'\n' || i_proc_name || E'\n' || i_param || E'\n' || i_sql_query || E'\n' || i_run_dttm_st || E'\n' || i_run_dttm_end || E'\n' || i_run_dttm_end - i_run_dttm_st || E'\n' || i_str_count;
	else 
	    INSERT INTO tech.tech_logs (
	    	flow_id,
	    	proc_name, 	
	    	param, 
	    	sql_query, 
	    	run_dttm_st,
	    	run_dttm_end,
	    	run_dttm_all,
	    	str_count
	    )
	    VALUES (
	    	i_flow_id,
	    	i_proc_name, 
	   		i_param, 
	   		i_sql_query, 
	   		i_run_dttm_st,
	   		i_run_dttm_end,
	   		i_run_dttm_end - i_run_dttm_st,
	   		i_str_count
	   	);
	end if;
	
   	raise notice 'procedure sp_add_log completed';
end;
$$;

