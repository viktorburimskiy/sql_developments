CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_COLLECT_STATISTICS(
	p_schema_nm varchar2,
	p_table_nm varchar2,
	p_job_id NUMBER,
	p_debug_mode boolean DEFAULT FALSE)
	
IS
 	v_sql_txt varchar2(1000);

BEGIN
	
	/*
	 * Description:
	 *	Процедура сбора статистики таблици по партиции
	 * Input param:
	 *  p_debug_mode    - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm     - имя схемы таблицы
	 *  p_table_nm      - имя таблицы
     *  p_job_id        - ид загрузки (unix_timestamp)
	 */

		
	v_sql_txt := '
		BEGIN
			DBMS_STATS.GATHER_TABLE_STATS( ownname =>  '''''' || p_schema_nm || ''''''
										  ,tabname =>  '''''' || p_table_nm  || ''''''
										  ,partname => '''''' || v_part_name || ''''''
										  ,estimate_percent => 10);
		END;';
		
	IF p_debug_mode THEN

		DBMS_OUTPUT.PUT_LINE(v_sql_txt);
	
	ELSE
		
		EXECUTE IMMEDIATE v_sql_txt;
	
	END IF;

EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;

