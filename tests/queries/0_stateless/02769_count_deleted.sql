DROP TABLE IF EXISTS data_02769;
CREATE TABLE IF NOT EXISTS data_02769 (x UInt64) ENGINE=MergeTree() ORDER BY x;

INSERT INTO data_02769 SELECT number FROM numbers(100);
SELECT 'system.parts with fresh inserted data';
SELECT name,rows,has_lightweight_delete,deleted_rows_count FROM system.parts WHERE table = 'data_02769' and active;

DELETE FROM data_02769 WHERE x = 50;

SELECT 'system.parts with lightweight deleted data';
SELECT name,rows,has_lightweight_delete,deleted_rows_count FROM system.parts WHERE table = 'data_02769' and active;

OPTIMIZE TABLE data_02769 FINAL;

SELECT 'system.parts with optimize final data';
SELECT name,rows,has_lightweight_delete,deleted_rows_count FROM system.parts WHERE table = 'data_02769' and active;

DROP TABLE IF EXISTS data_02769;
