<?php

include_once(__DIR__ . '/../common/init_db.php');

function check_cache($cycle, $partition_id)
{
  global $history_db;

  $stmt = $history_db->prepare('SELECT COUNT(*) FROM `replica_snapshot_cache` WHERE `run_id` = ?');
  $stmt->bind_param('i', $cycle);
  $stmt->bind_result($num_rows);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($num_rows == 0) {
    $query = 'INSERT INTO `replica_snapshot_cache`';
    $query .= ' SELECT ?, dd1.`site_id`, dd1.`dataset_id`, rs1.`id`, dd1.`id` FROM `deletion_decisions` AS dd1, `replica_size_snapshots` AS rs1';
    $query .= ' WHERE (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`) = (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`)';
    $query .= ' AND dd1.`partition_id` = ?';
    $query .= ' AND rs1.`size` != 0';
    $query .= ' AND rs1.`run_id` = (';
    $query .= '  SELECT MAX(rs2.`run_id`) FROM `replica_size_snapshots` AS rs2';
    $query .= '  WHERE (rs2.`site_id`, rs2.`partition_id`, rs2.`dataset_id`) = (rs1.`site_id`, rs1.`partition_id`, rs1.`dataset_id`)';
    $query .= '  AND rs2.`partition_id` = ?';
    $query .= '  AND rs2.`run_id` <= ?';
    $query .= ' )';
    $query .= ' AND dd1.`run_id` = (';
    $query .= '  SELECT MAX(dd2.`run_id`) FROM `deletion_decisions` AS dd2';
    $query .= '  WHERE (dd2.`site_id`, dd2.`partition_id`, dd2.`dataset_id`) = (dd1.`site_id`, dd1.`partition_id`, dd1.`dataset_id`)';
    $query .= '  AND dd2.`partition_id` = ?';
    $query .= '  AND dd2.`run_id` <= ?';
    $query .= ' )';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('iiiiii', $cycle, $partition_id, $partition_id, $cycle, $partition_id, $cycle);
    $stmt->execute();
    $stmt->close();
  }

  $stmt = $history_db->prepare('INSERT INTO `replica_snapshot_cache_usage` VALUES (?, NOW())');
  $stmt->bind_param('i', $cycle);
  $stmt->execute();
  $stmt->close();
}

?>
