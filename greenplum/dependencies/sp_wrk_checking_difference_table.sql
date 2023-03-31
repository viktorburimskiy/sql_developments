create or replace function tech.sp_wrk_checking_difference_table(i_debug_flag boolean, i_workflow_run_id bigint, i_ext_table_nm text, i_ods_schema_nm text, i_stg_schema_nm text) 
	returns integer
	language plpgsql
	volatile
as $body$
	
	/**
	 * Description:
	 * 		функция проверяет различие по столбцам между stg и ods таблицей. При обнаружении таковых, вызывается процедура sp_save_dependencies
	 *		select tech.sp_wrk_checking_difference_table(true,'e_01_02_t_cntry','ods','stg')
	 * 	
	 * Input:
	 * 	i_debug_flag - флаг дебага
	 * 	i_stg_table_nm - имя stg таблицы
	 *  	i_ods_schema_nm - имя ods схемы
	 *  	i_stg_schema_nm - имя stg схемы
	 * Dev: 
	 *	BurimskiyVA
	 *	25.11.2021
	 */

declare 

	l_ods_table_id int8;		-- oid ods таблицы
	l_change int4 default 0;	-- флаг наличия различия
	l_column_list text;		-- новый список колонок для вьюшки ods_view

begin

	-- проверяем наличие стобцов у ext и ods таблицы
	with ext_table as 
	(
		select 
			pc.oid as ext_table_id
			,pc.relname as ext_table_nm
			,substring(pc.relname,3) ext_table_re_nm
			,np.nspname as ext_schema_nm
			,a.attnum as ext_column_num
			,a.attname as ext_column_nm
			,format_type(a.atttypid, a.atttypmod) as ext_column_type
		from pg_catalog.pg_class pc
		join pg_catalog.pg_namespace np
			on pc.relnamespace = np.oid
		left join pg_catalog.pg_attribute a
			on a.attrelid = pc.oid 
			and a.attnum > 0 
			and not a.attisdropped
		where pc.relname = i_ext_table_nm
			and np.nspname = i_stg_schema_nm
	)
	, ods_table as 
	( 
		select 
			pc.oid as ods_table_id
			,pc.relname as ods_table_nm
			,substring(pc.relname,3) ods_table_re_nm
			,a.attnum as ods_column_num
			,a.attname as ods_column_nm
			,format_type(a.atttypid, a.atttypmod) as ods_column_type
		from pg_catalog.pg_class pc
		join pg_catalog.pg_namespace np
			on pc.relnamespace = np.oid
		left join pg_catalog.pg_attribute a
			on a.attrelid = pc.oid 
			and a.attnum > 0 
			and not a.attisdropped
		where pc.relname = 'o' || substring(i_ext_table_nm, 2)
			and np.nspname = i_ods_schema_nm
	), agg as 
	(
		select ex.ext_table_id
			,ex.ext_table_nm
			,od.ods_table_id
			,od.ods_table_nm
			,case when coalesce(ex.ext_column_num, 0) > coalesce(od.ods_column_num, 0)
				then ex.ext_column_num 
				else od.ods_column_num 
			end new_column_num
			, case 
				when ex.ext_column_nm is null and od.ods_column_nm in ('flow_id','flow_updated_dttm') then od.ods_column_nm
				when ex.ext_column_nm is null then 'null::' || ods_column_type || ' as ' || od.ods_column_nm 
				else ex.ext_column_nm
			end new_column_nm
			,case
				when ex.ext_column_nm is null and od.ods_column_nm in ('flow_id','flow_updated_dttm') then 0 
				when ex.ext_column_nm is null or od.ods_column_nm is null then 1 
				else 0 end change_flg
		from ext_table ex
		full join ods_table od
			on ex.ext_table_re_nm = od.ods_table_re_nm
			and ex.ext_column_nm = od.ods_column_nm
	)
	select 
		max(ods_table_id) ods_table_id
		,sum(change_flg) change_flg
		,string_agg(new_column_nm, ', ' order by new_column_num) column_list
	into 
		l_ods_table_id
		,l_change
		,l_column_list
	from agg;

	-- если есть различия по столбац между stg и ods, то запускаем процесс сохранения зависимых вьюшек
	if l_change > 0 then
		perform tech.sp_wrk_save_dependencies (i_debug_flag, i_workflow_run_id, l_ods_table_id, l_column_list, i_ods_schema_nm);
	end if;

	return l_change;

exception when others then 
	perform tech.sp_wrk_add_log(i_debug_flag, i_workflow_run_id, 'sp_wrk_checking_difference_table', 'ext_table_nm = ' || i_ext_table_nm || e'\nods_schema_nm = ' || i_ods_schema_nm  || e'\nstg_schema_nm = ' || i_stg_schema_nm, 'exception throw: ' || sqlstate || ' ' || sqlerrm, clock_timestamp(), clock_timestamp(), 0);
	return -1;
end;


$body$
execute on master;

