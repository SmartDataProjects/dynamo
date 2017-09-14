<?php

function check_cache($history_db, $cycle, $partition_id, $snapshot_archive_path)
{
  $stmt = $history_db->prepare('SELECT DATABASE()');
  $stmt->bind_result($db_name);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  $cache_db_name = $db_name . "_cache";
  $cache_table_name = "replicas_" . $cycle;

  $stmt = $history_db->prepare('SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?');
  $stmt->bind_param('ss', $cache_db_name, $cache_table_name);
  $stmt->bind_result($num_table);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($num_table == 0) {
    $srun = sprintf('%09d', $cycle);
    $xz_file_name = sprintf('%s/%s/%s/%s.db.xz', $snapshot_archive_path, substr($srun, 0, 3), substr($srun, 3, 3), $cache_table_name);
    if (!file_exists($xz_file_name))
      return false;

    exec('which unxz > /dev/null 2>&1', $stdout, $rc);
    if ($rc != 0)
      return false;

    $sqlite_file_name = sprintf('var/spool/dynamo/replica_snapshots/%s.db', $cache_table_name);
      
    exec(sprintf('unxz -k -c %s > %s', $xz_file_name, $sqlite_file_name));

    $snapshot_db = new SQLite3($sqlite_file_name);
    $sql = 'SELECT r.`site_id`, r.`dataset_id`, r.`size`, d.`value`, r.`condition` FROM `replicas` AS r';
    $sql .= ' INNER JOIN `decisions` AS d ON d.`id` = r.`decision_id`';
    $in_stmt = $snapshot_db->prepare($sql);
    $result = $in_stmt->execute();

    $history_db->query('CREATE TABLE `' . $cache_db_name . '`.`' . $cache_table_name . '` LIKE `replicas`');

    $stmt = $history_db->prepare('INSERT INTO `' . $cache_db_name . '`.`' . $cache_table_name . '` VALUES (?, ?, ?, ?, ?)');
    $stmt->bind_param('iiiii', $site_id, $dataset_id, $size, $decision, $condition);

    while (($arr = $result->fetchArray(SQLITE3_NUM))) {
      $site_id = $arr[0];
      $dataset_id = $arr[1];
      $size = $arr[2];
      $decision = $arr[3];
      $condition = $arr[4];

      $stmt->execute();
    }

    $in_stmt->close();
    $stmt->close();
    $snapshot_db->close();

    unlink($sqlite_file_name);
  }

  $stmt = $history_db->prepare('INSERT INTO `replica_snapshot_cache_usage` VALUES (?, NOW())');
  $stmt->bind_param('i', $cycle);
  $stmt->execute();
  $stmt->close();

  return true;
}

?>
