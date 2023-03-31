create or replace function tech.sys_audit_objects_function_text_return (p_schema_name text[])
	returns table (object_id oid, schema_name text, object_name text, arguments text, owner_name text, type_object text, description text, source_code text, creator_name text, create_dttm timestamp, modified_name text, modified_dttm timestamp)
	language plpgsql
	volatile
as $$

	/**
	 * Description:
	 * 	Формирование ddl объектов функций
	 * 	select * from tst.sys_audit_objects_function_text_return (ARRAY['tst', 'tech'])
	 *	select * from tst.sys_audit_objects_function_text_return ('{tst, tech}')
	 * 	
	 * Input:
	 * 	p_schema_name - список схем
	 *
	 */
	 
begin
	
	--raise notice 'array - %', p_schema_name;
	
	return query 
		select p.oid as object_id,
			cast(n.nspname as text) as schema_name
			,(n.nspname::text || '.'::text) || p.proname::text as object_name
			,pg_get_function_arguments(p.oid) as arguments
			,cast(pg_get_userbyid(p.proowner) as text) as owner_name
			,'f'::text as type_object
			,obj_description(p.oid) as description
			,concat('CREATE OR REPLACE FUNCTION ', n.nspname, '.', p.proname, '(', pg_get_function_arguments(p.oid), ') ', chr(10), chr(9)
				, 'RETURNS ', pg_get_function_result(p.oid), chr(10), chr(9)
				, 'LANGUAGE ', l.lanname, chr(10), chr(9)
				, case when p.prosecdef = true then 'SECURITY DEFINER' || chr(10) || chr(9) end,
				case
					when p.provolatile = 'i'::"char" then 'IMMUTABLE'::text
					when p.provolatile = 's'::"char" then 'STABLE'::text
					when p.provolatile = 'v'::"char" then 'VOLATILE'::text
					else null::text
				end, chr(10), 'as $body$', chr(10), p.prosrc, chr(10), '$body$', chr(10), 'EXECUTE ON ',
				case
					when p.proexeclocation = 'a'::"char" then 'ANY'::text
					when p.proexeclocation = 'm'::"char" then 'MASTER'::text
					when p.proexeclocation = 's'::"char" then 'ALL SEGMENTS'::text
					else null::text
				end, ';') as source_code
			,null::text as creator_name
			,null::timestamp as create_dttm
			,null::text as modified_name
			,null::timestamp as modified_dttm
		from pg_catalog.pg_proc p
		left join pg_catalog.pg_namespace n 
			on n.oid = p.pronamespace
		left join pg_catalog.pg_language l 
			on l.oid = p.prolang
		where n.nspname = any(p_schema_name);
end;
$$
execute on master;
