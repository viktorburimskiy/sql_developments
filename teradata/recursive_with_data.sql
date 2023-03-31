
CREATE MULTISET VOLATILE TABLE t1  AS (
SELECT
*
FROM
(
SELECT * FROM (SELECT NULL AS parent_id, 1 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 2 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 3 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 4 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 2 AS parent_id, 5 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 2 AS parent_id, 6 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 3 AS parent_id, 7 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 4 AS parent_id, 8 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id, 9 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 6 AS parent_id,10 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id,11 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id,12 AS unit_id) a
) b
)
WITH NO DATA 
PRIMARY INDEX (parent_id, unit_id) ON COMMIT PRESERVE ROWS;

INSERT INTO t1
SELECT
*
FROM
(
SELECT * FROM (SELECT NULL AS parent_id, 1 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 2 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 3 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 1 AS parent_id, 4 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 2 AS parent_id, 5 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 2 AS parent_id, 6 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 3 AS parent_id, 7 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 4 AS parent_id, 8 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id, 9 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 6 AS parent_id,10 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id,11 AS unit_id) a
UNION ALL
SELECT * FROM (SELECT 8 AS parent_id,12 AS unit_id) a
) b


WITH RECURSIVE cte (unit_id, par_id, Tree) AS
(
	SELECT unit_id, parent_id, Cast(unit_id AS VARCHAR(50)) AS Tree
	FROM t1
	WHERE parent_id IS NULL

	UNION ALL
	
	SELECT t1.unit_id, t1.parent_id , cte.Tree || '->' || Cast(t1.unit_id AS VARCHAR(100)) Tree
	FROM t1
	INNER JOIN cte 
		ON cte.unit_id = t1.parent_id
		AND Coalesce(cte.par_id,0) <> t1.unit_id
)

SELECT  *--Tree
FROM cte;







