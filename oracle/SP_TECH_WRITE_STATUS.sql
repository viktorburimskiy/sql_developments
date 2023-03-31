CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_WRITE_STATUS(
	p_debug_mode boolean DEFAULT FALSE,
	p_job_id NUMBER,
	p_schema_nm varchar2,
	p_table_nm varchar2,
	p_status_nm varchar2,
	p_status_dttm timestamp,
	p_monitoring_status varchar2)
	
IS

 	v_sql_txt varchar2(4000);

BEGIN

	/*
	 * Description:
	 *	Процедура записи общего статуса загрузки витрины (с момента запуска и окончание расчета либо ошибки)
	 * Input param:
	 *  p_debug_mode - режим запуска (выполнять или выводить результат в output)
	 * 	p_job_id - job_id виттрины
	 * 	p_schema_nm - имя схемы витрины
	 * 	p_table_nm - имя таблицы(витрины)
	 * 	p_status_nm - результат запуска (START/END)
	 * 	p_status_dttm - текущее системное время
	 * 	p_monitoring_status - статус витрины для мониторинга ?????
	 */

	v_sql_txt := 'INSERT INTO MY_USER.TECH_STATUS (
						job_id,
						schema_nm, 
						table_nm,
						status_nm,
						status_dttm,
						monitoring_status)
					VALUES (
						'|| p_job_id || ',
						'''|| p_schema_nm || ''',
						'''|| p_table_nm || ''',
						'''|| p_status_nm || ''',
						'''|| p_status_dttm || ''',
						'''|| p_monitoring_status || ''')';

	IF p_debug_mode THEN
		DBMS_OUTPUT.PUT_LINE('SQL - ' || v_sql_txt);
	ELSE
		EXECUTE IMMEDIATE v_sql_txt;
		COMMIT;
	END IF;

EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;

