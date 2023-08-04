CREATE OR REPLACE FUNCTION {TARGET_SCHEMA}.FN_CONVERT_UNIX_TO_DATE{SUFFIX} (p_job_id number)
RETURN DATE
IS
	v_date DATE;
BEGIN

	/* ver: {ver}
	 * Description:
	 * 	Функция конвертации unix_timestamp в date по Мск
	 * Input param:
	 * 	p_job_id 	- ид загрузки (unix_timestamp),
	 */

	SELECT (to_date('19700101', 'YYYYMMDD') + NUMTODSINTERVAL(p_job_id, 'SECOND')) + INTERVAL '3' HOUR AS MOSCOW_TIME
	INTO v_date
	FROM dual;

	RETURN v_date;
END;