CREATE OR REPLACE FUNCTION MY_USER.FN_GET_PARTITION_NAME (p_table_nm varchar2, p_job_id number)
RETURN VARCHAR2
IS

	v_sql_txt varchar2(4000);
	v_result  varchar2(200);

BEGIN
	/*
	 * Description:
	 *	Функция возвращает имя партиции таблицы
	 * Input param:
	 *  p_table_nm - имя таблицы(витрины)
	 * 	p_job_id   - ид загрузки (unix_timestamp)
	 */
	
	v_sql_txt :='
        WITH cte AS
        (
            SELECT partition_name
                , cast( EXTRACTVALUE( dbms_xmlgen.getxmltype('' SELECT high_value
                                                                FROM USER_TAB_PARTITIONS
                                                                WHERE table_name = '''''' || table_name || ''''''
                                                                    and partition_name = '''''' || partition_name || '''''''')
                                    , ''//text()'') AS NUMBER(20,0)) high_value
            FROM USER_TAB_PARTITIONS
            WHERE table_name = ''' || p_table_nm || '''
        )            
        SELECT partition_name
        FROM cte
        WHERE high_value = ' || p_job_id ||'';

    EXECUTE IMMEDIATE v_sql_txt INTO v_result;

    RETURN v_result;

END;
