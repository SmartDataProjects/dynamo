<?php

include_once(__DIR__ . '/../common/init_db.php');

$operation = 'deletion';

if (isset($TESTMODE) && $TESTMODE)
  $operation = 'deletion_test';

if (isset($_REQUEST['getPartitions']) && $_REQUEST['getPartitions']) {
  $data = array();
  
  $stmt = $history_db->prepare('SELECT DISTINCT `partitions`.`id`, `partitions`.`name` FROM `runs` INNER JOIN `partitions` ON `partitions`.`id` = `runs`.`partition_id` WHERE `runs`.`operation` LIKE ? ORDER BY `partitions`.`id`');
  $stmt->bind_param('s', $operation);
  $stmt->bind_result($id, $name);
  $stmt->execute();
  while ($stmt->fetch())
    $data[] = array('id' => $id, 'name' => $name);
  $stmt->close();

  echo json_encode($data);
  exit(0);
}

$cycle = 0;
$timestamp = '';
$partition_id = 0;

if (isset($_REQUEST['cycleNumber'])) {
  $cycle = 0 + $_REQUEST['cycleNumber'];
  $stmt = $history_db->prepare('SELECT `partition_id`, `time_start` FROM `runs` WHERE `id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ?');
  $stmt->bind_param('is', $cycle, $operation);
  $stmt->bind_result($partition_id, $timestamp);
  $stmt->execute();
  if (!$stmt->fetch())
    $cycle = 0;
  $stmt->close();
}
else if (isset($_REQUEST['partition'])) {
  $stmt = $history_db->prepare('SELECT `id` FROM `partitions` WHERE `name` LIKE ?');
  $stmt->bind_param('s', $_REQUEST['partition']);
  $stmt->bind_result($partition_id);
  $stmt->execute();
  if (!$stmt->fetch())
    $partition_id = 0;
  $stmt->close();
}
else if (isset($_REQUEST['partitionId']))
  $partition_id = 0 + $_REQUEST['partitionId'];

if ($cycle == 0) {
  if ($partition_id == 0)
    $partition_id = 1;

  $stmt = $history_db->prepare('SELECT `id`, `time_start` FROM `runs` WHERE `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ? ORDER BY `id` DESC LIMIT 1');
  $stmt->bind_param('is', $partition_id, $operation);
  $stmt->bind_result($cycle, $timestamp);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['searchDataset']) && $_REQUEST['searchDataset']) {
  $dataset_pattern = str_replace('*', '%', $_REQUEST['datasetName']);

  // existing replicas
  $query = 'SELECT st.`name`, ds.`name`, sn.`size` * 1.e-9, dl.`decision`, dl.`reason`';
  $query .= ' FROM `deletion_decisions` AS dl';
  $query .= ' INNER JOIN `replica_snapshots` AS sn ON sn.`id` = dl.`snapshot_id`';
  $query .= ' INNER JOIN `datasets` AS ds ON ds.`id` = sn.`dataset_id`';
  $query .= ' INNER JOIN `sites` AS st ON st.`id` = sn.`site_id`';
  $query .= ' WHERE dl.`run_id` = ? AND ds.`name` LIKE ?';
  $query .= ' ORDER BY st.`name`';

  $stmt = $history_db->prepare($query);
  $stmt->bind_param('is', $cycle, $dataset_pattern);
  $stmt->bind_result($site_name, $dataset_name, $size, $decision, $reason);
  $stmt->execute();

  $json = '[';

  $sname = '';
  while ($stmt->fetch()) {
    if ($site_name != $sname) {
      if (strlen($json) != 1)
        $json .= ']},';

      $sname = $site_name;
      $json .= '{"name":"' . $sname . '","datasets":[';
    }
    else
      $json .= ',';

    $json .= sprintf('{"name":"%s","size":%f,"decision":"%s","reason":"%s"}', $dataset_name, $size, $decision, $reason);
  }
  if (strlen($json) != 1)
    $json .= ']}';
  $json .= ']';
  $stmt->close();

  echo $json;
  exit(0);  
}

$stmt = $history_db->prepare('SELECT `id` FROM `runs` WHERE `id` > ? AND `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ? ORDER BY `id` ASC LIMIT 1');
$stmt->bind_param('iis', $cycle, $partition_id, $operation);
$stmt->bind_result($next_cycle);
$stmt->execute();
if (!$stmt->fetch())
  $next_cycle = 0;
$stmt->close();

$stmt = $history_db->prepare('SELECT `id` FROM `runs` WHERE `id` < ? AND `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ? ORDER BY `id` DESC LIMIT 1');
$stmt->bind_param('iis', $cycle, $partition_id, $operation);
$stmt->bind_result($prev_cycle);
$stmt->execute();
if (!$stmt->fetch())
  $prev_cycle = 0;
$stmt->close();

if (isset($_REQUEST['getData']) && $_REQUEST['getData']) {
  // main data structure
  $data = array();

  $data['operation'] = $operation;
  $data['cycleNumber'] = $cycle;
  $data['previousCycle'] = $prev_cycle;
  $data['nextCycle'] = $next_cycle;
  $data['cycleTimestamp'] = $timestamp;
  $data['partition'] = $partition_id;

  if ($_REQUEST['dataType'] == 'summary') {
    $index_to_id = array();
    $site_names = array();
    
    $stmt = $history_db->prepare('SELECT `id`, `name` FROM `sites` ORDER BY `name`');
    $stmt->bind_result($id, $name);
    $stmt->execute();
    while ($stmt->fetch()) {
      $index_to_id[] = $id;
      $site_names[$id] = $name;
    }
    $stmt->close();

    $quotas = array();
    $stmt = $history_db->prepare('SELECT `site_id`, `quota` FROM `quota_snapshots` WHERE `partition_id` = ? AND `run_id` <= ? ORDER BY `run_id` DESC');
    $stmt->bind_param('ii', $partition_id, $cycle);
    $stmt->bind_result($site_id, $quota);
    $stmt->execute();
    while ($stmt->fetch()) {
      if (array_key_exists($site_id, $quotas))
        continue;

      $quotas[$site_id] = $quota;

      if (count($quotas) == count($data['sites']))
        break;
    }
    $stmt->close();

    $statuses = array();
    $stmt = $history_db->prepare('SELECT `site_id`, `active`, `status` FROM `site_status_snapshots` WHERE `run_id` <= ? ORDER BY `run_id` DESC');
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($site_id, $active, $status);
    $stmt->execute();
    while ($stmt->fetch()) {
      if (array_key_exists($site_id, $statuses))
        continue;

      if ($status == 'morgue' || $status == 'waitroom' || $active == 0)
        $statuses[$site_id] = 0;
      else if ($active == 2)
        $statuses[$site_id] = 2;
      else
        $statuses[$site_id] = 1;

      if (count($statuses) == count($data['sites']))
        break;
    }
    $stmt->close();

    $site_total = array('protect' => array(), 'keep' => array(), 'delete' => array(), 'protectPrev' => array(), 'keepPrev' => array());

    // existing replicas
    $stmt = $history_db->prepare('SELECT s.`site_id`, d.`decision`, SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? GROUP BY s.`site_id`, d.`decision`');
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] = $size;
    $stmt->close();

    // past replicas
    $stmt = $history_db->prepare('SELECT s.`site_id`, d.`decision`, SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `deleted_replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? GROUP BY s.`site_id`, d.`decision`');
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] += $size;
    $stmt->close();
 
    // existing replicas - Prev
    $stmt = $history_db->prepare('SELECT s.`site_id`, CONCAT(d.`decision`, \'Prev\'), SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? GROUP BY s.`site_id`, d.`decision`');
    $stmt->bind_param('i', $prev_cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] = $size;
    $stmt->close();
 
    // past replicas - Prev
    $stmt = $history_db->prepare('SELECT s.`site_id`, CONCAT(d.`decision`, \'Prev\'), SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `deleted_replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? GROUP BY s.`site_id`, d.`decision`');
    $stmt->bind_param('i', $prev_cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] += $size;
    $stmt->close();

    $data['siteData'] = array();
    foreach ($index_to_id as $id) {
      $data['siteData'][] =
        array(
              'id' => $id,
              'name' => $site_names[$id],
              'quota' => $quotas[$id],
              'status' => $statuses[$id],
              'protect' => 0. + $site_total['protect'][$id],
              'keep' => 0. + $site_total['keep'][$id],
              'delete' => 0. + $site_total['delete'][$id],
              'protectPrev' => 0. + $site_total['protectPrev'][$id],
              'keepPrev' => 0. + $site_total['keepPrev'][$id]
              );
    }
  }
  else if ($_REQUEST['dataType'] == 'siteDetail') {
    $site_name = $_REQUEST['siteName'];

    $data['name'] = $site_name;

    $data['datasets'] = array();

    // existing replicas
    $query = 'SELECT ds.`name`, sn.`size` * 1.e-9, dl.`decision`, dl.`reason` FROM `deletion_decisions` AS dl';
    $query .= ' INNER JOIN `replica_snapshots` AS sn ON sn.`id` = dl.`snapshot_id`';
    $query .= ' INNER JOIN `datasets` AS ds ON ds.`id` = sn.`dataset_id`';
    $query .= ' INNER JOIN `sites` AS st ON st.`id` = sn.`site_id`';
    $query .= ' WHERE dl.`run_id` = ? AND st.`name` LIKE ?';
    $query .= ' ORDER BY ds.`name`';
    $stmt = $history_db->prepare($query);
    $stmt->bind_param('is', $cycle, $site_name);
    $stmt->bind_result($name, $size, $decision, $reason);
    $stmt->execute();
    while ($stmt->fetch())
      $data['datasets'][] = array('name' => $name, 'size' => $size, 'decision' => $decision, 'reason' => $reason);
    $stmt->close();

    // deleted replicas
    $query = 'SELECT ds.`name`, sn.`size` * 1.e-9, dl.`decision`, dl.`reason` FROM `deletion_decisions` AS dl';
    $query .= ' INNER JOIN `deleted_replica_snapshots` AS sn ON sn.`id` = dl.`snapshot_id`';
    $query .= ' INNER JOIN `datasets` AS ds ON ds.`id` = sn.`dataset_id`';
    $query .= ' INNER JOIN `sites` AS st ON st.`id` = sn.`site_id`';
    $query .= ' WHERE dl.`run_id` = ? AND st.`name` LIKE ?';
    $query .= ' ORDER BY ds.`name`';
    $stmt = $history_db->prepare($query);
    $stmt->bind_param('is', $cycle, $site_name);
    $stmt->bind_result($name, $size, $decision, $reason);
    $stmt->execute();
    while ($stmt->fetch())
      $data['datasets'][] = array('name' => $name, 'size' => $size, 'decision' => $decision, 'reason' => $reason);
    $stmt->close();
  }

  echo json_encode($data);
}
else {
  $html = file_get_contents(__DIR__ . '/html/detox.html');

  $html = str_replace('${CYCLE_NUMBER}', "$cycle", $html);
  $html = str_replace('${PARTITION}', "$partition_id", $html);

  echo $html;
}

?>
