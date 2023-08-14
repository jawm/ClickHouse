#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Firstly, we'll create a part, delete from it, and assert there is a correct count of how many deleted rows exist in the part.
# Then again, after optimizing, check the amount of deleted rows (should then be back at zero)
${CLICKHOUSE_CLIENT} -n --query "
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
"

# Now we will simulate the backward compatibility case by editing `count.txt` so it is in the old format, containing only a count of rows in the part (deleted or not)
path=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.parts WHERE table='data_02769' and name='all_1_1_1_2'")
${CLICKHOUSE_CLIENT} -n --query "SELECT * FROM system.storage_policies;"
${CLICKHOUSE_CLIENT} -n --query "ALTER TABLE data_02769 DETACH PART 'all_1_1_1_2';"
cat ${path}count.txt