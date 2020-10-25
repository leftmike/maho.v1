--
-- Test SELECT WHERE with indexes
--
-- {{Sort .Global false}}

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (
    c1 int primary key,
    c2 int,
    c3 int,
    c4 int,
    c5 int
);

CREATE INDEX idx1 ON tbl1 (c2 DESC);

CREATE INDEX idx2 ON tbl1 (c3, c4);

CREATE UNIQUE INDEX idx3 ON tbl1 (c5);

INSERT INTO tbl1 (c1, c2, c3, c4, c5) VALUES
    (1, 10, 100, -1, -1),
    (2, 10, 100, -2, -2),
    (3, 20, 100, -2, -3),
    (4, 20, 200, -3, -4),
    (5, 20, 200, -4, -5),
    (6, 30, 300, -5, -6),
    (7, 40, 300, -5, -7),
    (8, 40, 300, -5, -8),
    (9, 40, 300, -6, -9),
    (10, 50, 400, -6, -10),
    (11, 60, 500, -6, -11),
    (12, 70, 600, -6, -12),
    (13, 80, 700, -7, -13);

SELECT * FROM tbl1;

SELECT * FROM tbl1@idx1;

SELECT * FROM tbl1@idx2;

SELECT * FROM tbl1@idx3;

EXPLAIN SELECT * FROM tbl1 WHERE c1 = 5;

EXPLAIN SELECT * FROM tbl1@idx1 WHERE c2 = 5;
