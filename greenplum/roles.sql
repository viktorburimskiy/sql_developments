			
    
select 
       r."oid"
       ,array(      select pr.rolname
             from pg_catalog.pg_auth_members m
             join pg_catalog.pg_roles pr
                    on m.roleid = pr.oid
             where m."member" = r.oid) as include_group
       ,r.rolname
       ,r.rolsuper
       ,r.rolinherit inherit
       ,r.rolcreaterole
       ,r.rolcreatedb
       ,r.rolcatupdate
       ,r.rolcanlogin
       ,r.rolconnlimit
       ,r.rolvaliduntil
       ,r.rolconfig
       --,r.rolresqueue
       ,r.rolcreaterextgpfd create_r_ext_gpfd
       ,r.rolcreaterexthttp create_r_ext_http
       ,r.rolcreatewextgpfd create_w_ext_gpfd
       ,r.rolresgroup
from pg_catalog.pg_roles r
where r.rolname = 'Burimskiy-va'
order by rolname




/* Проверка привелегий для таблиц*/
select
	pc.relname 
	,coalesce (s[1], '') as grantee
	,case when s[2] = 'arwdDxt' 
		then 'all'
		else (	select string_agg(privilege, ', ' order by privilege)
				from (	select 	case t0
									when 'r' then 'select'
									when 'w' then 'update'
									when 'a' then 'insert'
									when 'd' then 'delete'
									when 'D' then 'truncate'
									when 'x' then 'references'
									when 't' then 'trigger'
--									when 'X' then 'execute'
--									when 'U' then 'usage'
--									when 'C' then 'create'
--									when 'c' then 'connect'
								end as privilege
						from regexp_split_to_table(s[2], '') t0 
					) as t1
			) 
	end as privilege
	, s[2]
from pg_catalog.pg_class as pc
join pg_catalog.pg_namespace as pn
	on pc.relnamespace = pn.oid
join pg_catalog.pg_roles as pr
	on pr.oid = pc.relowner,
unnest (coalesce(pc.relacl::text[], format('{%s=arwdDxt/%s}', pr.rolname, pr.rolname)::text[])) as acl,
regexp_split_to_array(acl, '=|/') as s 
where pn.nspname = 'test'		--схема
	--and s[1] = 'r_grnplm_ld_risk_ldgprisk_stg_r'		--роль
	and pc.relname = 'test'
	and pc.relkind ='r'
	and not exists (select 1
					from pg_catalog.pg_inherits pi1
					left join pg_catalog.pg_inherits pi2 
						on pi1.inhparent = pi2.inhrelid 
					where coalesce (pi1.inhrelid, pi2.inhrelid) = pc.oid)	
order by pc.relname 



/* Проверка привелегий для функций*/
select
	pp.proname 
	,coalesce (s[1], '') as grantee
	,case when s[2] = 'arwdDxt' 
		then 'all'
		else (	select string_agg(privilege, ', ' order by privilege)
				from (	select 	case t0
									when 'X' then 'execute'
								end as privilege
						from regexp_split_to_table(s[2], '') t0 
					) as t1
			) 
	end as privilege
	, s[2]
from pg_catalog.pg_proc as pp
join pg_catalog.pg_namespace as pn
	on pp.pronamespace = pn.oid
join pg_catalog.pg_roles as pr
	on pr.oid = pp.proowner,
unnest (coalesce(pp.proacl::text[], format('{%s=arwdDxt/%s}', pr.rolname, pr.rolname)::text[])) as acl,
regexp_split_to_array(acl, '=|/') as s 
where proname = 'test'



--X
--arwdDxt
select distinct
	pp.proname 
--	,coalesce (s[1], '') as grantee
	,s[2]
from pg_catalog.pg_proc as pp
join pg_catalog.pg_namespace as pn
	on pp.pronamespace = pn.oid
join pg_catalog.pg_roles as pr
	on pr.oid = pp.proowner,
unnest (coalesce(pp.proacl::text[], format('{%s=arwdDxt/%s}', pr.rolname, pr.rolname)::text[])) as acl,
regexp_split_to_array(acl, '=|/') as s 
where s[2] = 'arwdDxt'
and pn.nspname = 'test'



select *
from pg_catalog.pg_extprotocol pe 




