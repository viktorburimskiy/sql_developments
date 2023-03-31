CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_TRANC_TABLE(
	p_schema_nm varchar2,
	p_table_nm varchar2,
	p_debug_mode boolean DEFAULT FALSE)
	
IS
 	v_sql_txt varchar2(1000);

BEGIN
	
	/*
	 * Description:
	 *	Процедура очистки таблицы
	 * Input param:
	 *  p_debug_mode - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm - имя схемы таблицы
	 *  p_table_nm - имя таблицы
	 */
	
	v_sql_txt := '
		TRUNCATE TABLE ' || p_schema_nm || '.' || p_table_nm;
		
	IF p_debug_mode THEN

		DBMS_OUTPUT.PUT_LINE(v_sql_txt);
	
	ELSE
		
		EXECUTE IMMEDIATE v_sql_txt;
	
	END IF;

EXCEPTION 
	WHEN OTHERS THEN
	
	RAISE;

END;

