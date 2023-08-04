CREATE OR REPLACE PROCEDURE {TARGET_SCHEMA}.SP_TECH_WRITE_STATUS{SUFFIX}(
	p_debug_mode boolean DEFAULT FALSE,
	p_job_id NUMBER,
	p_schema_nm varchar2,
	p_airflow_job_id NUMBER,
	p_table_nm varchar2,
	p_status_nm varchar2,
	p_monitoring_status varchar2)

IS

 	v_sql_txt_ins varchar2(4000);
 	v_sql_txt_del varchar2(4000);

BEGIN

	/*  ver: {ver}
	 *  Description:
	 *	Процедура записи общего статуса загрузки витрины (с момента запуска и окончание расчета либо ошибки)
	 *  Input param:
	 *  p_debug_mode 		- режим запуска (выполнять или выводить результат в output)
	 * 	p_job_id 			- ид загрузки (unix_timestamp)
	 * 	p_schema_nm 		- имя схемы витрины
	 * 	p_table_nm 			- имя таблицы(витрины)
	 * 	p_status_nm 		- результат запуска (NEW/START/END)
	 * 	current_timestamp 	- текущее системное время
	 * 	p_monitoring_status - статус витрины для мониторинга OK/ERROR_CRIT/ERROR/WARNING
	 */

	v_sql_txt_del := '
		DELETE {TARGET_SCHEMA}.TECH_STATUS{SUFFIX}
		WHERE
			job_id ='|| p_job_id || '
			AND schema_nm = '''|| p_schema_nm || '''
			AND table_nm = '''|| p_table_nm || '''';


	v_sql_txt_ins := '
		INSERT INTO {TARGET_SCHEMA}.TECH_STATUS (
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
			current_timestamp,
			'''|| p_monitoring_status || ''')';

	IF p_debug_mode THEN

		DBMS_OUTPUT.PUT_LINE('SQL TECH_STATUS DELETE - ' || v_sql_txt_del);
		DBMS_OUTPUT.PUT_LINE('SQL TECH_STATUS INSERT - ' || v_sql_txt_ins);

	ELSE
		EXECUTE IMMEDIATE v_sql_txt_del;
		EXECUTE IMMEDIATE v_sql_txt_ins;

		COMMIT;
	END IF;

EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;