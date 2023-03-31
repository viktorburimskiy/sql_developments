create or replace function tech.sp_wrk_execute(i_debug_flg boolean, i_sql_txt text, i_workflow_id bigint) 
	returns bigint
	language plpgsql

as $$
	
       /* 
        * Description:
        * 	select tech.sp_wrk_execute(true, 'select 1 from meta.table_etl_info', 0)
    	* 	i_workflow_id int8 default 0
    	* 	Процедура sp_wrk_execute принимает на вход дебаг мод и строку sql запроса
    	* 	в случае если i_debug_flg = true, процедура просто выводит данный i_sql_txt в консоль, иначе выполнять запрос с помощью execute
    	*  
    	* Input:
    	* 	i_debug_flg - флаг дебага
    	* 	i_sql_txt - sql запрос для выполнения
    	*/ 

declare 
	l_str_count int8 default 0; -- кол-во строк, записанных запросом
begin
	if (i_debug_flg) then 
		raise notice '%', i_sql_txt;
	else 
		execute i_sql_txt;
		get diagnostics l_str_count = row_count;
	end if;

	return l_str_count;

exception when others then 
	-- для проброса в родительскую процедуру
	raise exception 'exception throw: %', sqlstate || ' ' || sqlerrm;
end;
$$;


