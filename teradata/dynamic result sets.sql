REPLACE  PROCEDURE "tst".Burimskiy_test (IN x INTEGER, IN str ARRAY) DYNAMIC RESULT SETS 1
BEGIN
	DECLARE curl1 CURSOR WITH RETURN ONLY FOR SELECT Current_Date() - x, Current_Date();
	OPEN curl1;
END;



CALL "001_mis_retail_channel_lab".Burimskiy_test(1)



SELECT str_id, str_seq, str_name
FROM TABLE (StrTok_Split_To_Table(1, '12,33433,34,655,8', ',')
	RETURNS(str_id INTEGER, str_seq INTEGER, str_name VARCHAR(20)))AS dt




WITH tbl AS
	(
		SELECT evt_id, nazn_new
		FROM "001_mis_retail_channel_lab".data_parser_operisk
		WHERE evt_id =337919949541
	)
SELECT str_id, str_seq, str_name
FROM TABLE (StrTok_Split_To_Table(Cast(tbl.evt_id AS VARCHAR(10)), tbl.nazn_new, ';')
	RETURNS(str_id VARCHAR(10), str_seq INTEGER, str_name VARCHAR(256)))AS dt
