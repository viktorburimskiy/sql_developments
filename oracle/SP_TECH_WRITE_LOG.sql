CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_WRITE_LOG(
	p_debug_mode boolean DEFAULT FALSE,
	p_job_id NUMBER,
	p_schema_nm varchar2,
	p_param_nm varchar2,
	p_proc_nm varchar2,
	p_step varchar2,
	p_status_nm varchar2,
	p_run_start_dttm timestamp DEFAULT current_timestamp,
	p_run_end_dttm timestamp DEFAULT current_timestamp,
	p_row_cnt NUMBER DEFAULT 0)
IS

 	v_sql_txt varchar2(4000);
 	v_all_dttm INTERVAL DAY TO SECOND;

BEGIN

	/*
	 * Description:
	 *	Процедура записи лога
	 * Input param:
	 *  p_debug_mode - режим запуска (выполнять или выводить результат в output)
	 * 	p_job_id - job_id виттрины
	 * 	p_param_nm - параметры запуска процедуры витрины
	 * 	p_step - выполняемый шаг
	 * 	p_status_nm - результат запуска (OK/ERR: massege)
	 * 	run_start_dttm - время начала
	 * 	run_end_dttm - время окончание
	 * 	row_cnt - кол-во обработанных строк (INSERT/UPDATE/DELETE)
	 */

	v_all_dttm := p_run_end_dttm - p_run_start_dttm;
	v_sql_txt := 'INSERT INTO MY_USER.TECH_LOGS (
						job_id,
						schema_nm, 
						param_nm,
						proc_nm,
						step_nm,
						status_nm,
						run_start_dttm,
						run_end_dttm,
						run_all_dttm,
						row_cnt)
					VALUES (
						'|| p_job_id || ',
						'''|| p_schema_nm || ''',
						'''|| p_param_nm || ''',
						'''|| p_proc_nm || ''',
						'''|| p_step || ''',
						'''|| p_status_nm || ''',
						'''|| p_run_start_dttm || ''',
						'''|| p_run_end_dttm   || ''',
						'''|| v_all_dttm || ''',
						'|| p_row_cnt || ')';

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

