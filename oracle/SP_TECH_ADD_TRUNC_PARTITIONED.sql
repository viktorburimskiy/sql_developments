CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_ADD_TRUNC_PARTITIONED(
	p_schema_nm varchar2,
	p_table_nm varchar2, 
	p_job_id NUMBER,
	p_debug_mode boolean DEFAULT true)
	
IS
 	v_sql_txt 			varchar2(1000);
 	v_sql_txt_part_add	varchar2(1000);
	v_sql_txt_part_trun	varchar2(1000);
 	v_cnt				NUMBER(10) := 0;

BEGIN
	
	/*
	 * Description:
	 *	Процедура проверяет наличие партиции в таблице - при отсутствии создает партцию по знаечнию JOB_ID, при наличии партиции отчишает для нового датасета
	 * Input param:
	 *  p_debug_mode    - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm     - имя схемы таблицы
	 *  p_table_nm      - имя таблицы (витрины)
	 * 	p_job_id        - ид загрузки (unix_timestamp)
	 */
	
	
	v_sql_txt := '
			SELECT 
				COUNT(1)
			FROM ALL_TAB_PARTITIONS t
			WHERE t.TABLE_OWNER = ''' || p_schema_nm || '''
				AND t.TABLE_NAME = ''' || p_table_nm || '''
				AND CAST(substr(t.PARTITION_NAME, 2) AS number(10)) = ' || p_job_id;
			
	v_sql_txt_part_add := '
			ALTER TABLE ' || p_schema_nm || '.' || p_table_nm || ' ADD PARTITION P' || p_job_id || ' VALUES(' || p_job_id || ')';
		
	v_sql_txt_part_trun := '
			ALTER TABLE ' || p_schema_nm || '.' || p_table_nm || ' TRUNCATE PARTITION (P' || p_job_id || ')';
			
	IF p_debug_mode THEN
	
		DBMS_OUTPUT.PUT_LINE(v_sql_txt);
		DBMS_OUTPUT.PUT_LINE(v_sql_txt_part_add);
		DBMS_OUTPUT.PUT_LINE(v_sql_txt_part_trun);
	
	ELSE
		
		EXECUTE IMMEDIATE v_sql_txt INTO v_cnt;
	
		IF v_cnt = 0 THEN
		
			EXECUTE IMMEDIATE v_sql_txt_part_add;
		
		ELSE
		
			EXECUTE IMMEDIATE v_sql_txt_part_trun;
		
		END IF;
	
	END IF;
	
	
EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;

