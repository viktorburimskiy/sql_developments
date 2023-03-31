
--анонимные функции
do $body$ 	
	declare v1 text;
	declare v2 text;
	declare v3 text;
	declare v4 text;

	begin
		select count(1) from tst.test_table_ola;
		
		insert into tst.test_table_ola (row_id) values (999);
	
		insert into tst.test_table_olap2 values (123456789, 'text');
	exception when others then
		get stacked diagnostics v1 = message_text, 
					v2 = returned_sqlstate,
					v3 = table_name,
					v4 = pg_exception_context;
		raise notice 'SQLSTATE % | % | % | %', v1, v2, v3, v4;
	end;
$body$ language plpgsql
	

