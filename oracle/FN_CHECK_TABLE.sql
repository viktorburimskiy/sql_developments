CREATE OR REPLACE FUNCTION MY_USER.FN_CHECK_TABLE(
	p_schema_nm IN varchar2,
	p_table_nm IN varchar2,
	p_debug_mode IN boolean DEFAULT FALSE) RETURN NUMBER

IS

 	v_sql_txt varchar2(4000);
 	v_cnt NUMBER(1);

BEGIN
	
	/*
	 * Description:
	 *	Функция проверки наличия таблицы
	 * Input param:
	 *  p_debug_mode - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm - имя схемы таблицы
	 *  p_table_nm - имя таблицы (витрины)
	 */
	
	v_sql_txt := 'SELECT COUNT(1) cnt FROM ALL_TABLES WHERE OWNER = '''|| p_schema_nm || ''' AND TABLE_NAME = ''' || p_table_nm || '''';
	
	IF p_debug_mode THEN
	
		DBMS_OUTPUT.PUT_LINE(v_cnt);
	
	ELSE
		
		EXECUTE IMMEDIATE v_sql_txt INTO v_cnt;
		
		RETURN v_cnt;
	
	END IF;

EXCEPTION
	WHEN OTHERS THEN
	
	    RETURN -1;
END;

