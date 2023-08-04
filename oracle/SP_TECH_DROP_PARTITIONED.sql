CREATE OR REPLACE PROCEDURE MY_USER.SP_TECH_DROP_PARTITIONED(
	p_schema_nm varchar2,
	p_table_nm varchar2, 
	p_cnt_part NUMBER,
	p_debug_mode boolean DEFAULT FALSE)
	
IS
 	v_sql_txt_part_drop varchar2(1000);

BEGIN
	
	/*
	 * Description:
	 *	Процедура удаляет все младшие партиции. Оставляет только последние партиции в кол-ве p_cnt_part штук
	 * Input param:
	 *  p_debug_mode    - режим запуска (выполнять или выводить результат в output)
	 *  p_schema_nm     - имя схемы таблицы
	 *  p_table_nm      - имя таблицы
	 *  p_cnt_part      - кол-во партиций которое необходимо сохранять
	 */
	
	
	FOR current_part IN (
		WITH cte_part AS 
		(
			SELECT 
				CAST(substr(t.PARTITION_NAME, 2) AS number(10)) as PARTITION_VALUE
				,rownum AS rn
			FROM ALL_TAB_PARTITIONS t
			WHERE t.PARTITION_NAME <> 'P0'
				AND t.TABLE_OWNER = p_schema_nm
				AND t.TABLE_NAME = p_table_nm
			ORDER BY PARTITION_VALUE DESC
		)
		SELECT PARTITION_VALUE
		FROM 
		(
			SELECT PARTITION_VALUE
				,ROWNUM AS rn
			FROM cte_part
		) t
		WHERE rn > p_cnt_part)
		
	LOOP 
		
		v_sql_txt_part_drop := '
			ALTER TABLE ' || p_schema_nm || '.' || p_table_nm || ' DROP PARTITION FOR(' || current_part.PARTITION_VALUE || ')';
		
		IF p_debug_mode THEN
	
		DBMS_OUTPUT.PUT_LINE(v_sql_txt_part_drop);
		
		ELSE
			
			EXECUTE IMMEDIATE v_sql_txt_part_drop;
		
		END IF;
		
	END LOOP;

	COMMIT;

EXCEPTION
    WHEN OTHERS THEN

        RAISE;
END;

