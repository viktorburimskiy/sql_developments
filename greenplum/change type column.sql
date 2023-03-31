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
where pc.relname = 'account_saldo_plan_oper_proc'				--имя схемы
	and pn.nspname = 's_grnplm_ld_risk_ldgprisk_dirm_oprisk'	--имя таблицы
	and format_type(a.atttypid, a.atttypmod) = 'text'			--тип заменяемого поля

	
ALTER TABLE s_grnplm_ld_risk_ldgprisk_dirm_oprisk.account_saldo_plan_oper_proc ALTER COLUMN "account_type" TYPE varchar;
ALTER TABLE s_grnplm_ld_risk_ldgprisk_dirm_oprisk.account_saldo_plan_oper_proc ALTER COLUMN "account_type_sec" TYPE varchar;
ALTER TABLE s_grnplm_ld_risk_ldgprisk_dirm_oprisk.account_saldo_plan_oper_proc ALTER COLUMN "id_loan_saldo" TYPE varchar;
ALTER TABLE s_grnplm_ld_risk_ldgprisk_dirm_oprisk.account_saldo_plan_oper_proc ALTER COLUMN "id_loan_po" TYPE varchar;
ALTER TABLE s_grnplm_ld_risk_ldgprisk_dirm_oprisk.account_saldo_plan_oper_proc ALTER COLUMN "day" TYPE varchar;