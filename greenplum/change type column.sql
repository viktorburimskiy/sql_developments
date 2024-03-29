select
	pn.nspname
	,pc.relname	
	,a.attname
	,row_number () over (partition by pc.oid order by a.attnum) as num
	,format_type(a.atttypid, a.atttypmod)
	,concat ('ALTER TABLE '
			,pn.nspname
			,'.'
			,pc.relname::text
			,' ALTER COLUMN "'
			,a.attname
			,'" TYPE '
			,'varchar;'
			) as full_text
from pg_catalog.pg_class pc
join pg_catalog.pg_namespace pn
	on pn.oid = pc.relnamespace
left join pg_catalog.pg_attribute a
	on a.attrelid = pc.oid 
	and a.attnum > 0 
	and not a.attisdropped
left join pg_catalog.pg_attrdef ad
	on ad.adrelid = a.attrelid
	and ad.adnum = a.attnum
	and a.atthasdef
where pc.relname = 'oper_proc'				--имя схемы
	and pn.nspname = 'rm_oprisk'	--имя таблицы
	and format_type(a.atttypid, a.atttypmod) = 'text'			--тип заменяемого поля

