CREATE OR REPLACE PROCEDURE MY_USER.SP_ETL_TEST(
	p_schema_nm varchar2,
	p_table_nm varchar2,
	p_job_id NUMBER,
	p_cnt_part NUMBER,
	p_debug_mode boolean DEFAULT FALSE)
	
IS
 	v_desc varchar2(1000);
 	v_start_dttm timestamp;
 	v_etl_name varchar2(50) := 'ETL_TEST';
 	v_status_nm varchar2(1000) := 'OK';
 	v_param varchar2(1000) := p_schema_nm || '; ' || p_table_nm || '; ' || p_job_id || '; ' || p_cnt_part;
 	v_table_final_nm varchar2(50) := p_table_nm || '_FINAL';
 	v_table_full_nm varchar2(50) := p_table_nm || '_FULL';
 	v_sql_txt varchar2(4000);

BEGIN
	
	/*
	 * Description:
	 *	Процедура загрузки таблицы TEST_FINAL в TEST_FULL
	 * Input param:
	 *  p_debug_mode    - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm     - имя схемы таблицы
	 *  p_table_nm      - имя таблицы
	 *  p_cnt_part      - кол-во партиций которое необходимо сохранять
	 */

	v_start_dttm := current_timestamp;

	MY_USER.SP_TECH_WRITE_STATUS(p_debug_mode, p_job_id, p_schema_nm, p_table_nm, 'START', current_timestamp, '...');
	
	v_desc := 'Step 1: - Clear ' || v_table_final_nm;

		BEGIN
			
			MY_USER.SP_TECH_TRANC_TABLE(p_schema_nm, v_table_final_nm, p_debug_mode);
		
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
		
		EXCEPTION 
			WHEN OTHERS THEN
			
			v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
			RAISE;
			
		END;
	
	v_start_dttm := current_timestamp;
	v_desc := 'Step 2: - Load data to ' || v_table_final_nm;

		BEGIN
			
			v_sql_txt := ' 
				INSERT /*+ append*/ ALL 
					INTO MY_USER.TEST_FINAL (JOB_ID, ID, TEXT, CREATE_DTTM) VALUES (' || p_job_id || ', ' || p_job_id || '+1, ''a' || p_job_id || ''', SYSTIMESTAMP)
					INTO MY_USER.TEST_FINAL (JOB_ID, ID, TEXT, CREATE_DTTM) VALUES (' || p_job_id || ', ' || p_job_id || '+2, ''a' || p_job_id || ''', SYSTIMESTAMP)
					INTO MY_USER.TEST_FINAL (JOB_ID, ID, TEXT, CREATE_DTTM) VALUES (' || p_job_id || ', ' || p_job_id || '+3, ''a' || p_job_id || ''', SYSTIMESTAMP)
				SELECT 1 FROM DUAL';
					
			IF p_debug_mode THEN
			
				DBMS_OUTPUT.PUT_LINE(v_sql_txt);
			
			ELSE
			
				EXECUTE IMMEDIATE v_sql_txt;
				
			END IF;
		
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp, SQL%ROWCOUNT);

			COMMIT;
			
		EXCEPTION WHEN OTHERS THEN
		
			v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
			RAISE;
		
		END;
	
	v_start_dttm := current_timestamp;
	v_desc := 'Step 3: - Exchange partition ' || v_table_final_nm || ' with ' || v_table_full_nm;

		BEGIN
			
			MY_USER.SP_TECH_ADD_TRUNC_PARTITIONED(p_schema_nm, v_table_full_nm, p_job_id, p_debug_mode);
		
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
		
			MY_USER.SP_TECH_EXCHANGE_PARTITIONED(p_schema_nm, v_table_full_nm, v_table_final_nm, p_job_id, p_debug_mode);

		EXCEPTION WHEN OTHERS THEN
		
			v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
			RAISE;
		
		END;
	
	v_start_dttm := current_timestamp;
	v_desc := 'Step 4: - Drop last partition ' || v_table_full_nm;

		BEGIN
			
			MY_USER.SP_TECH_DROP_PARTITIONED(p_schema_nm, v_table_full_nm, p_cnt_part, p_debug_mode);
			
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
		
		EXCEPTION WHEN OTHERS THEN
		
			v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
			RAISE;
		
		END;
	
	v_start_dttm := current_timestamp;
	v_desc := 'Step 5: - Сollect statistics ' || v_table_full_nm;

		BEGIN
			
			MY_USER.SP_TECH_COLLECT_STATISTICS(p_schema_nm, v_table_full_nm, p_debug_mode);
			
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
		
		EXCEPTION WHEN OTHERS THEN
		
			v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
			MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
			RAISE;

		END;

		MY_USER.SP_TECH_WRITE_STATUS(p_debug_mode, p_job_id, p_schema_nm, p_table_nm, 'END', current_timestamp, '...');

EXCEPTION
    WHEN OTHERS THEN
    
--    	v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
--		
--    	MY_USER.SP_TECH_WRITE_LOG(p_debug_mode, p_job_id, p_schema_nm, v_param, v_etl_name, v_desc, v_status_nm, v_start_dttm, current_timestamp);
--		
--		DBMS_OUTPUT.PUT_LINE('ошибку обработал в конце');
    
--        RAISE_APPLICATION_ERROR(SQLCODE, SQLERRM);
		RAISE;
END;

