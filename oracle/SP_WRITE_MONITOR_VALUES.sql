CREATE OR REPLACE PROCEDURE {TARGET_SCHEMA}.SP_WRITE_MONITOR_VALUES(
	p_debug_mode boolean DEFAULT TRUE,
	p_trg_schema_name varchar2,
	p_trg_table_name varchar2,
	p_airflow_job_id NUMBER,
	p_mon_shema_name varchar2,
	p_mon_table_name varchar2,
	p_job_id NUMBER
    )

IS
    v_desc_log varchar2(1000);
	v_sql_text varchar2(4000);
	v_result varchar2(4000);
	v_start_dttm timestamp;
	v_end_dttm timestamp;
	v_run_dttm INTERVAL DAY TO SECOND;
    v_status_nm varchar2(1000) := 'OK';
    v_etl_name varchar2(50) := 'MONITORING';
    v_param varchar2(1000) := p_mon_shema_name || '; ' || p_mon_table_name || '; ' || p_job_id;
	v_business_date date := {TARGET_SCHEMA}.FN_CONVERT_UNIX_TO_DATE(p_job_id);

BEGIN

	/* ver: {ver}
	 * Description:
	 * 	Процедура подсчета метрик по объектам и запись результата в таблицу Z_MONITOR_VALUES
	 * Input param:
	 * 	p_debug_mode 		- режим запуска (выполнять или выаодить в результат в Output). По умолчанию - вывод в Output
	 *	p_trg_schema_name 	- имя целевой схемы где храниться таблица Z_MONITOR_VALUES
	 *	p_trg_table_name 	- имя целевой таблицы Z_MONITOR_VALUES
	 *	p_mon_shema_name 	- имя схемы в который размещена таблица по которой строяться статистики
	 *	p_mon_table_name 	- имя таблицы по которой строяться статистики (без _FULL)
	 *	p_job_id 			- ид загрузки (unix_timestamp)
	 *	p_airflow_job_id 	- инстанс airflow в виде строки
	 *	v_business_date 	- дата по которой береться срез для витрин)
	 *
	 * Example RUN:
	 *	BEGIN SP_WRITE_MONITOR_VALUES(False, 'TEST_PIMDM1', 'Z_MONITOR_VALUES', 10, 'PROD_DM_RKK', 'DOCUMENT', 1667250000); END;
	 */
    BEGIN
        v_start_dttm := current_timestamp;
        v_desc_log := 'Step : Run MONITORING for -> ' || v_param;
        {TARGET_SCHEMA}.SP_TECH_WRITE_LOG{SUFFIX}(p_debug_mode, p_job_id, v_param, p_airflow_job_id, v_etl_name, v_desc_log, v_status_nm, v_start_dttm, current_timestamp);

        v_sql_text :=
                'UPDATE ' || p_trg_schema_name || '.' || p_trg_table_name || '
                    SET STAT_OK = ''NOT''
                WHERE TABLE_NAME = ''' || p_mon_table_name || '''
                    and JOB_ID = ''' || p_job_id || '''';

        IF p_debug_mode THEN
            DBMS_OUTPUT.PUT_LINE('WRITE metric: ' || v_sql_text);
        ELSE
            EXECUTE IMMEDIATE v_sql_text;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
        v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
        {TARGET_SCHEMA}.SP_TECH_WRITE_LOG{SUFFIX}(p_debug_mode, p_job_id, v_param, p_airflow_job_id, v_etl_name, v_desc_log, v_status_nm, v_start_dttm, current_timestamp);
        RAISE;
    END;
	FOR current_row IN (
			SELECT p.SCHEMA_NAME, p.TABLE_NAME, p.FIELD_NAME, p."FILTER", l.STAT_NAME, l.SQLTEXT
			FROM Z_MONITOR_PLAN p
			JOIN Z_MONITOR_LIST l
				ON p.STAT_NAME = l.STAT_NAME
			WHERE p.SCHEMA_NAME = p_mon_shema_name
				AND p.TABLE_NAME = p_mon_table_name
			ORDER BY p.FIELD_NAME, l.STAT_NAME
		)
	LOOP

		--заменяем переменные в строке выполнения (так проще, чем с USING)
		v_sql_text:= REPLACE(current_row.SQLTEXT, '(stat_name)', current_row.STAT_NAME);
		v_sql_text:= REPLACE(v_sql_text, '(schema)', 			 p_trg_schema_name);
		v_sql_text:= REPLACE(v_sql_text, '(table_name)', 		 current_row.TABLE_NAME || '_FULL');
		v_sql_text:= REPLACE(v_sql_text, '(field_name)', 		 current_row.FIELD_NAME);
		v_sql_text:= REPLACE(v_sql_text, '(filter)', 			 current_row."FILTER");
		v_sql_text:= REPLACE(v_sql_text, '(job_id)', 			 p_job_id);

		v_start_dttm := current_timestamp;

		--расчитываем метрику для атрибута витрины
		EXECUTE IMMEDIATE v_sql_text INTO v_result;

		IF p_debug_mode THEN
			DBMS_OUTPUT.PUT_LINE('RUN metric: ' || v_sql_text);
			DBMS_OUTPUT.PUT_LINE('RESULT metric: ' || v_result);
		END IF;

		v_end_dttm := current_timestamp;
		v_run_dttm := v_end_dttm - v_start_dttm;

		--собираем строку для записи метрики в итоговю таблицу
		v_sql_text :=
			'INSERT INTO ' || p_trg_schema_name || '.' || p_trg_table_name || '
				(
					JOB_ID
					,PROCESSING_DATE
					,BUSINESS_DATE
					,SCHEMA_NAME
					,TABLE_NAME
					,FIELD_NAME
					,STAT_NAME
					,STAT_VALUE
					,STAT_OK
					,RUN_TIME
					,PROCESSING_DATE_END
					,AIRFLOW_JOB_ID
				)
				VALUES
				(
				''' || p_job_id || ''',
                to_timestamp(replace(''' || v_start_dttm || ''', '','', ''.''), ''DD.MM.YY HH24:MI:SS.FF''),
				TO_DATE(''' || v_business_date || ''', ''DD-MM-YY''),
				''' || p_trg_schema_name || ''',
				''' || current_row.TABLE_NAME || ''',
				''' || current_row.FIELD_NAME || ''',
				''' || current_row.STAT_NAME || ''',
				''' || v_result || ''',
				''OK'',
				''' || v_run_dttm || ''',
				to_timestamp(replace(''' || v_end_dttm || ''', '','', ''.''), ''DD.MM.YY HH24:MI:SS.FF''),
				''' || p_airflow_job_id || ''')';


		IF p_debug_mode THEN
			DBMS_OUTPUT.PUT_LINE('WRITE metric: ' || v_sql_text);
		ELSE
			EXECUTE IMMEDIATE v_sql_text;
		END IF;

	END LOOP;
    BEGIN
        v_start_dttm := current_timestamp;
        v_desc_log := 'Step : End MONITORING for -> ' || v_param;
        {TARGET_SCHEMA}.SP_TECH_WRITE_LOG{SUFFIX}(p_debug_mode, p_job_id, v_param, p_airflow_job_id, v_etl_name, v_desc_log, v_status_nm, v_start_dttm, current_timestamp);
    EXCEPTION
        WHEN OTHERS THEN
        v_status_nm := 'ERROR: ' || SQLCODE || '; ' || SQLERRM;
        {TARGET_SCHEMA}.SP_TECH_WRITE_LOG{SUFFIX}(p_debug_mode, p_job_id, v_param, p_airflow_job_id, v_etl_name, v_desc_log, v_status_nm, v_start_dttm, current_timestamp);
        RAISE;
    END;
EXCEPTION
	WHEN OTHERS THEN
		RAISE;
END SP_WRITE_MONITOR_VALUES;