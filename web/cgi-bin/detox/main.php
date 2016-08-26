<?php

include_once(__DIR__ . '/../common/init_db.php');
include(__DIR__ . '/check_cache.php');

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
$policy_version = '';
$timestamp = '';
$partition_id = 0;

if (isset($_REQUEST['cycleNumber'])) {
  $cycle = 0 + $_REQUEST['cycleNumber'];
  $stmt = $history_db->prepare('SELECT `partition_id`, `policy_version`, `time_start` FROM `runs` WHERE `id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ?');
  $stmt->bind_param('is', $cycle, $operation);
  $stmt->bind_result($partition_id, $policy_version, $timestamp);
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

  $stmt = $history_db->prepare('SELECT `id`, `policy_version`, `time_start` FROM `runs` WHERE `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ? ORDER BY `id` DESC LIMIT 1');
  $stmt->bind_param('is', $partition_id, $operation);
  $stmt->bind_result($cycle, $policy_version, $timestamp);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['checkUpdate']) && $_REQUEST['checkUpdate']) {
  echo $cycle;
  exit(0);
}

check_cache($cycle, $partition_id);

if (isset($_REQUEST['searchDataset']) && $_REQUEST['searchDataset']) {
  $dataset_pattern = str_replace('*', '%', $_REQUEST['datasetName']);

  $query = 'SELECT s.`name`, d.`name`, r.`size` * 1.e-9, l.`decision`, l.`reason`';
  $query .= ' FROM `replica_snapshot_cache` AS c';
  $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
  $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
  $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
  $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
  $query .= ' WHERE c.`run_id` = ? AND d.`name` LIKE ?';

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
  $data['cyclePolicy'] = $policy_version;
  $data['cycleTimestamp'] = $timestamp;
  $data['partition'] = $partition_id;

  if ($_REQUEST['dataType'] == 'summary') {
    $index_to_id = array();
    $site_names = array();
    
    $quotas = array();

    $query = 'SELECT `site_id`, `quota` FROM `quota_snapshots` AS q1';
    $query .= ' WHERE `partition_id` = ? AND `run_id` = (';
    $query .= '  SELECT MAX(`run_id`) FROM `quota_snapshots` AS q2';
    $query .= '   WHERE q2.`site_id` = q1.`site_id` AND q2.`partition_id` = ? AND q2.`run_id` <= ?';
    $query .= ' )';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('iii', $partition_id, $partition_id, $cycle);
    $stmt->bind_result($site_id, $quota);
    $stmt->execute();
    while ($stmt->fetch()) {
      if ($quota != 0)
        $quotas[$site_id] = $quota;
    }
    $stmt->close();

    $query = sprintf('SELECT `id`, `name` FROM `sites` WHERE `id` IN (%s) ORDER BY `name`', implode(',', array_keys($quotas)));
    $stmt = $history_db->prepare($query);
    $stmt->bind_result($id, $name);
    $stmt->execute();
    while ($stmt->fetch()) {
      $index_to_id[] = $id;
      $site_names[$id] = $name;
    }
    $stmt->close();

    $statuses = array();

    $query = 'SELECT `site_id`, `active`, `status` FROM `site_status_snapshots` AS s1';
    $query .= ' WHERE `run_id` = (';
    $query .= '  SELECT MAX(`run_id`) FROM `site_status_snapshots` AS s2';
    $query .= '   WHERE s2.`site_id` = s1.`site_id` AND s2.`run_id` <= ?';
    $query .= ' )';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($site_id, $active, $status);
    $stmt->execute();
    while ($stmt->fetch()) {
      if ($status == 'morgue' || $status == 'waitroom' || $active == 0)
        $statuses[$site_id] = 0;
      else if ($active == 2)
        $statuses[$site_id] = 2;
      else
        $statuses[$site_id] = 1;
    }
    $stmt->close();

    $site_total = array('protect' => array(), 'keep' => array(), 'delete' => array(), 'protectPrev' => array(), 'keepPrev' => array());

    $query = 'SELECT c.`site_id`, l.`decision`, SUM(r.`size`) * 1.e-12';
    $query .= ' FROM `replica_snapshot_cache` AS c';
    $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
    $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
    $query .= ' WHERE c.`run_id` = ?';
    $query .= ' GROUP BY c.`site_id`, l.`decision`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] = $size;
    $stmt->close();

    check_cache($prev_cycle, $partition_id);

    $query = 'SELECT c.`site_id`, CONCAT(l.`decision`, \'Prev\'), SUM(r.`size`) * 1.e-12';
    $query .= ' FROM `replica_snapshot_cache` AS c';
    $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
    $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
    $query .= ' WHERE c.`run_id` = ?';
    $query .= ' GROUP BY c.`site_id`, l.`decision`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $prev_cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch())
      $site_total[$decision][$site_id] = $size;
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

    $query = 'SELECT d.`name`, r.`size` * 1.e-9, l.`decision`, p.`text`';
    $query .= ' FROM `replica_snapshot_cache` AS c';
    $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
    $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
    $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
    $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
    $query .= ' INNER JOIN `policy_conditions` AS p ON p.`id` = l.`matched_condition`';
    $query .= ' WHERE c.`run_id` = ? AND s.`name` LIKE ?';
    $query .= ' ORDER BY r.`size` DESC';

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
