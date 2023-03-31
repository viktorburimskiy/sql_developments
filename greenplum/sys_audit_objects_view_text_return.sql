create or replace function tech.sys_audit_objects_view_text_return (p_schema_name text[])
	returns table (object_id oid, schema_name text, object_name text, arguments text, owner_name text, type_object text, description text, source_code text, creator_name text, create_dttm timestamp, modified_name text, modified_dttm timestamp)
	language plpgsql
	volatile
as $$

	/**
	 * Description:
	 * 	ормирование ddl объектов представлений
	 * 	select * from tst.sys_audit_objects_view_text_return (ARRAY['tst', 'tech'])
	 *	select * from tst.sys_audit_objects_view_text_return ('{tst, tech}')
	 * 	
	 * Input:
	 * 	p_schema_name - список схем
	 *
	 */

begin
	
	--raise notice 'array - %', p_schema_name;
	
	return query 
		select 
			(vi.schemaname || '.' || vi.viewname)::regclass::oid as object_id
			,cast(vi.schemaname as text) as schema_name
			,vi.schemaname || '.' || vi.viewname as object_name
			,null::text as arguments
			,cast(vi.viewowner as text)  as owner_name
			,'v'::text as type_object
			,obj_description((vi.schemaname || '.' || vi.viewname)::regclass::oid) as description
			,concat('CREATE OR REPLACE VIEW ', vi.schemaname || '.' || vi.viewname, chr(10), 'as ', chr(10), vi.definition) as source_code
			,cast(cr_oper.stausename as text)  as creator_name
			,cr_oper.statime::timestamp as create_dttm
			,cast(al_oper.stausename as text)  as modified_name
			,al_oper.statime::timestamp as modified_dttm
		from pg_views vi
		left join pg_stat_last_operation cr_oper 
			on cr_oper.objid = (vi.schemaname || '.' || vi.viewname)::regclass::oid 
			and cr_oper.staactionname = 'CREATE'
		left join ( 
				select 
					pg_stat_last_operation.objid
					,pg_stat_last_operation.stausename
					,pg_stat_last_operation.statime
					,row_number() over (partition by pg_stat_last_operation.objid order by pg_stat_last_operation.statime desc) as rn
				from pg_stat_last_operation
				where (pg_stat_last_operation.staactionname not in ('CREATE', 'TRUNCATE') and pg_stat_last_operation.stausename <> 'gpadmin')) al_oper 
			on al_oper.objid = (vi.schemaname || '.' || vi.viewname)::regclass::oid 
			and al_oper.rn = 1
		where vi.schemaname = any(p_schema_name);
end;
$$
execute on master;
