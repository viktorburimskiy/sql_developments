create or replace function public.aeo_test_trans(i int4)
       returns float4
       language plpgsql
as $$


declare
		 k real;
		 m_txt text;

begin
	insert into public.aeo_log_test(dttm,message) values (now(),'1 BEGIN SUCCES step-1 i:'||i );

			begin
			  SELECT 10/i INTO k;
			  --RETURN k;
			  insert into public.aeo_log_test(dttm,message) values (now(),'1 END SUCCES step-1 i:'||i );
			exception when division_by_zero then
			  get stacked diagnostics
			  m_txt = MESSAGE_TEXT;
			  insert into public.aeo_log_test(dttm,message) values (now(),'1 Error step-1:'||m_txt);
			return -1;
			end;


	insert into public.aeo_log_test(dttm,message) values (now(),'2 BEGIN SUCCES step-2 i:'||(i-1) );

			begin
			  SELECT 10/(i-1) INTO k;
			  insert into public.aeo_log_test(dttm,message) values (now(),'2 END SUCCES step-2 i:'||(i-1) );
			  --RETURN k;
			exception when division_by_zero then
			  get stacked diagnostics
			  m_txt = MESSAGE_TEXT;
			  insert into public.aeo_log_test(dttm,message) values (now(),'2 Error step-2:'||m_txt);
			return -1;
			end;


	return 0;
end;



$$
execute on any;



[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select * from public.aeo_log_test order by 1,2;'
 dttm | message
------+---------
(0 rows)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select public.aeo_test_trans(2);'
 aeo_test_trans
----------------
              0
(1 row)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select * from public.aeo_log_test order by 1,2;'
            dttm            |          message
----------------------------+---------------------------
 2020-09-18 11:59:21.590816 | 1 BEGIN SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 1 END SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 2 BEGIN SUCCES step-2 i:1
 2020-09-18 11:59:21.590816 | 2 END SUCCES step-2 i:2
(4 rows)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select public.aeo_test_trans(1);'
 aeo_test_trans
----------------
             -1
(1 row)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select * from public.aeo_log_test order by 1,2;'
            dttm            |             message
----------------------------+---------------------------------
 2020-09-18 11:59:21.590816 | 1 BEGIN SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 1 END SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 2 BEGIN SUCCES step-2 i:1
 2020-09-18 11:59:21.590816 | 2 END SUCCES step-2 i:2
 2020-09-18 12:00:11.738933 | 1 BEGIN SUCCES step-1 i:1
 2020-09-18 12:00:11.738933 | 1 END SUCCES step-1 i:1
 2020-09-18 12:00:11.738933 | 2 BEGIN SUCCES step-2 i:0
 2020-09-18 12:00:11.738933 | 2 Error step-2:division by zero
(8 rows)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select public.aeo_test_trans(0);'
 aeo_test_trans
----------------
             -1
(1 row)

[gpadmin@mdw ~/aniskevich1-eo]$ psql -c 'select * from public.aeo_log_test order by 1,2;'
            dttm            |             message
----------------------------+---------------------------------
 2020-09-18 11:59:21.590816 | 1 BEGIN SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 1 END SUCCES step-1 i:2
 2020-09-18 11:59:21.590816 | 2 BEGIN SUCCES step-2 i:1
 2020-09-18 11:59:21.590816 | 2 END SUCCES step-2 i:2
 2020-09-18 12:00:11.738933 | 1 BEGIN SUCCES step-1 i:1
 2020-09-18 12:00:11.738933 | 1 END SUCCES step-1 i:1
 2020-09-18 12:00:11.738933 | 2 BEGIN SUCCES step-2 i:0
 2020-09-18 12:00:11.738933 | 2 Error step-2:division by zero
 2020-09-18 12:00:27.175716 | 1 BEGIN SUCCES step-1 i:0
 2020-09-18 12:00:27.175716 | 1 Error step-1:division by zero
(10 rows)

[gpadmin@mdw ~/aniskevich1-eo]$
