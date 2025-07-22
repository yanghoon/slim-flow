show catalogs;
-- show create catalog iceberg;
use catalog iceberg;
show current catalog;

show databases;
create database my_db;
use my_db;
show current database;

show tables;
CREATE TABLE test_table (id BIGINT, data STRING);
INSERT INTO test_table VALUES (1, 'foo'), (2, 'bar');
SELECT * FROM test_table;
