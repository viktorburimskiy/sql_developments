CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_EXCHANGE_PARTITIONED(
	p_schema_nm varchar2,
	p_table_full_nm varchar2, 
	p_table_final_nm varchar2,
	p_job_id NUMBER,
	p_debug_mode boolean DEFAULT true)
	
IS
 	v_sql_txt 			varchar2(1000);

BEGIN
	
	/*
	 * Description:
	 *	Процедура выполняет преобразование не партиционированной таблицы FINAL в раздел партиции таблицы FULL
	 * Input param:
	 *  p_debug_mode - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm - имя схемы таблицы
	 *  p_table_full_nm - имя таблицы FULL
	 *  p_table_final_nm - имя таблицы FINAL
	 * 	p_job_id - job_id витрины (значение по которому формируются партиции)
	 */
	
	
	v_sql_txt := '
		ALTER TABLE ' || p_schema_nm || '.' || p_table_full_nm || ' EXCHANGE PARTITION FOR(' || p_job_id || ') WITH TABLE '  || p_schema_nm || '.' || p_table_final_nm || ' WITH VALIDATION';
			
	IF p_debug_mode THEN
	
		DBMS_OUTPUT.PUT_LINE(v_sql_txt);
	
	ELSE
		
		EXECUTE IMMEDIATE v_sql_txt;
	
	END IF;
	
	
EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;

