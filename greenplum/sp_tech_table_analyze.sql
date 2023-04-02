create or replace function public.sys_table_analyze(tbl "text")
	returns void
	language plpgsql
	security definer 
	volatile
as $$
	
       
begin

	execute 'analyze public.' || tbl;

	exception when others then

	raise exception using message = sqlerrm;
	
end;


$$
execute on master;
