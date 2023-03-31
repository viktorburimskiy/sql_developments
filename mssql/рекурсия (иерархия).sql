--select *
--from  [Temp_data].[BurimskyVA].test_table_rec 

WITH Parent AS
(
    SELECT
        ID,
        pid,
        title
    FROM
        [Temp_data].[BurimskyVA].test_table_rec 
    WHERE
        pid IS NULL

    UNION ALL

    SELECT
        TH.ID,
        TH.pid,
        CONVERT(varchar(256), Parent.title + '/' + TH.title) AS Path
    FROM
        [Temp_data].[BurimskyVA].test_table_rec TH
    INNER JOIN
        Parent
    ON
        Parent.ID = TH.pid
)
SELECT * FROM Parent 