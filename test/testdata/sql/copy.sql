--
-- Test COPY
--

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (c1 int primary key, c2 text, c3 int default 0);

COPY tbl1 (c1, c2) FROM stdin DELIMITER '|';
1|One
2|Two
3|Three
4|\N
5|Five
\.

SELECT * FROM tbl1;

COPY tbl1 (c1, c2, c3) FROM stdin;
6	Six	6
7	Seven	7
8	\N	8
9	\N	\N
\.

SELECT * FROM tbl1;
