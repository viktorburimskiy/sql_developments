create or replace function s_grnplm_as_salesntwrk_pcap_sn_data.sys_table_analyze(tbl "text")
	returns void
	language plpgsql
	security definer 
	volatile
as $$
	
       
begin

	execute 'analyze s_grnplm_as_salesntwrk_pcap_sn_data.' || tbl;

	exception when others then

	raise exception using message = sqlerrm;
	
end;


$$
execute on master;
