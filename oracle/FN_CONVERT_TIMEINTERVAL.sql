CREATE OR REPLACE FUNCTION MY_USER.FN_CONVERT_TIMEINTERVAL (p_interval INTERVAL DAY TO SECOND)
RETURN numeric
IS

	v_sec numeric;

BEGIN
    /*
  	 * Description:
  	 *	
  	 * Input param:
  	 *  p_interval    - р
  	 */
       SELECT
	       EXTRACT(DAY FROM p_interval) * 86400
	       	+ EXTRACT(HOUR FROM p_interval) * 3600
	       	+ EXTRACT(MINUTE FROM p_interval) * 60
			+ EXTRACT(SECOND FROM p_interval)
       INTO v_sec
       FROM dual;

       RETURN v_sec;
END ;
