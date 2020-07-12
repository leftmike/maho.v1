--
-- Test SHOW CONSTRAINTS FROM table
--

DROP TABLE IF EXISTS tbl1;

CREATE TABLE tbl1 (
    c1 int primary key,
    c2 int check (c2 >= 10),
    c3 int unique,
    c4 int not null,
    c5 int default -1,
    unique (c2 desc, c3)
);

SELECT * FROM metadata.constraints where table_name = 'tbl1';


