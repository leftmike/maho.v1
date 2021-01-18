--
-- Test
--     SHOW CONSTRAINTS FROM table
--     ALTER TABLE table ...
--

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (
    c1 int primary key,
    c2 int check (c2 >= 10),
    c3 int unique,
    c4 int not null,
    c5 int default -1,
    c6 int constraint con1 check (c6 >= 100),
    c7 int constraint con2 not null,
    c8 int constraint con3 default 99 constraint con4 not null,
    c9 int constraint con5 default 11,
    unique (c2 desc, c3)
);

SELECT * FROM metadata.constraints where table_name = 'tbl1';

SHOW CONSTRAINTS FROM tbl1;

{{Fail .Test}}
ALTER TABLE tbl1 DROP CONSTRAINT con99;

ALTER TABLE tbl1 DROP CONSTRAINT IF EXISTS con99;

ALTER TABLE tbl1 DROP CONSTRAINT con2;

{{Fail .Test}}
ALTER TABLE tbl1 DROP CONSTRAINT con2;

SHOW CONSTRAINTS FROM tbl1;

{{Fail .Test}}
ALTER TABLE tbl1 ALTER c1 DROP DEFAULT;

{{Fail .Test}}
ALTER TABLE tbl1 ALTER c2 DROP NOT NULL;

ALTER TABLE tbl1 ALTER c5 DROP DEFAULT;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 ALTER c4 DROP NOT NULL;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 DROP CONSTRAINT check_1;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 DROP CONSTRAINT con1;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 ALTER c8 DROP NOT NULL;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 ALTER c8 DROP DEFAULT;

SHOW CONSTRAINTS FROM tbl1;

ALTER TABLE tbl1 DROP CONSTRAINT con5;

SHOW CONSTRAINTS FROM tbl1;

{{Fail .Test}}
ALTER TABLE tbl1 ALTER c1 DROP NOT NULL;

SHOW CONSTRAINTS FROM tbl1;
