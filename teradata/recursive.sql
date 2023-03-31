WITH RECURSIVE datelist
(
    dt_beg,
    dt_end
 )
AS
(
    SELECT
        dt_beg,
        dt_end
    FROM (
            SELECT
                To_Date('2020-01-12', 'yyyy-mm-dd')        AS dt_beg,
                To_Date('2020-01-12', 'yyyy-mm-dd') +1    AS dt_end
            ) inp
    UNION ALL
    SELECT
        dt_beg + 1, 
        dt_end + 1
    FROM datelist
    WHERE
        dt_beg < To_Date('2020-01-16', 'yyyy-mm-dd') 
 )
SELECT
    dt_beg,
    dt_end
FROM datelist
ORDER BY
    dt_beg DESC
	
