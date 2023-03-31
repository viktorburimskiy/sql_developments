SHOW PROCEDURE "001_mis_retail_channel_lab".prc_Burimskiy_test;

REPLACE  PROCEDURE "001_mis_retail_channel_lab".Burimskiy_test ( i INTEGER, OUT vSum INTEGER)
BEGIN
	--DECLARE vSum INTEGER;
	SET vSum = i + i;
END;

DROP PROCEDURE  "001_mis_retail_channel_lab".prc_Burimskiy_test
