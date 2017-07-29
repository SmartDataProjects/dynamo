<?php

include_once(__DIR__ . '/../common/db_conf.php');
include(__DIR__ . '/check_cache.php');

date_default_timezone_set('America/New_York');

$history_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamohistory');

$operation = 'deletion';

if (isset($TESTMODE) && $TESTMODE)
  $operation = 'deletion_test';

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'getPartitions') {
  $data = array();
  
  $stmt = $history_db->prepare('SELECT DISTINCT `partitions`.`id`, `partitions`.`name` FROM `runs` INNER JOIN `partitions` ON `partitions`.`id` = `runs`.`partition_id` WHERE `runs`.`operation` LIKE ? ORDER BY `partitions`.`id`');
  $stmt->bind_param('s', $operation);
  $stmt->bind_result($id, $name);
  $stmt->execute();
  while ($stmt->fetch()) {
    if ((!isset($TESTMODE) || !$TESTMODE) && ($name == 'AnalysisOps' || $name == 'DataOps'))
      continue;

    $elem = array('id' => $id, 'name' => $name);
    if ($name == 'Physics')
      array_unshift($data, $elem);
    else
      $data[] = $elem;
  }
  $stmt->close();

  echo json_encode($data);
  exit(0);
}
else if(isset($_REQUEST['command']) && $_REQUEST['command'] == 'findCycle') {
  $phedex = isset($_REQUEST['phedex']) ? $_REQUEST['phedex'] + 0 : 0;

  $cycle = 0;

  $stmt = $history_db->prepare('SELECT `run_id` FROM `deletion_requests` WHERE `id` = ?');
  $stmt->bind_param('i', $phedex);
  $stmt->bind_result($cycle);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  echo '{"cycle": ' . $cycle . '}';
  exit(0);
}

$cycle = 0;
$policy_version = '';
$comment = '';
$timestamp = '';
$partition_id = 0;

if (isset($_REQUEST['cycleNumber'])) {
  $cycle = 0 + $_REQUEST['cycleNumber'];
  $stmt = $history_db->prepare('SELECT `partition_id`, `policy_version`, `comment`, `time_start` FROM `runs` WHERE `id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ?');
  $stmt->bind_param('is', $cycle, $operation);
  $stmt->bind_result($partition_id, $policy_version, $comment, $timestamp);
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
    $partition_id = 10;

  $stmt = $history_db->prepare('SELECT `id`, `policy_version`, `comment`, `time_start` FROM `runs` WHERE `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` LIKE ? ORDER BY `id` DESC LIMIT 1');
  $stmt->bind_param('is', $partition_id, $operation);
  $stmt->bind_result($cycle, $policy_version, $comment, $timestamp);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'checkUpdate') {
  echo $cycle;
  exit(0);
}

check_cache($history_db, $cycle, $partition_id);

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'searchDataset') {
  // return
  // {"results": [{"siteData": [{"name": site, "datasets": [datasets]}]}], "conditions": [conditions]}

  $query = 'SELECT s.`name`, d.`name`, r.`size` * 1.e-9, l.`decision`, l.`matched_condition`';
  $query .= ' FROM `replica_snapshot_cache` AS c';
  $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
  $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
  $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
  $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
  $query .= ' WHERE c.`run_id` = ? AND d.`name` LIKE ?';

  $results_data = array();

  if (isset($_REQUEST['datasetNames']) && is_array($_REQUEST['datasetNames'])) {
    foreach ($_REQUEST['datasetNames'] as $pattern) {
      $dataset_pattern = str_replace('_', '\_', $pattern);
      $dataset_pattern = str_replace('*', '%', $dataset_pattern);
      $dataset_pattern = str_replace('?', '_', $dataset_pattern);
 
      $stmt = $history_db->prepare($query);
      $stmt->bind_param('is', $cycle, $dataset_pattern);
      $stmt->bind_result($site_name, $dataset_name, $size, $decision, $condition_id);
      $stmt->execute();
  
      $condition_ids = array();

      $json_data = array();
  
      while ($stmt->fetch()) {
        if (!array_key_exists($site_name, $json_data))
          $json_data[$site_name] = array();
  
        $json_data[$site_name][] = sprintf('{"name":"%s","size":%f,"decision":"%s","conditionId":"%s"}', $dataset_name, $size, $decision, $condition_id);
        $condition_ids[] = $condition_id;
      }
      $stmt->close();
  
      $json_texts = array();
      foreach ($json_data as $site_name => $datasets)
        $json_texts[] = sprintf('{"name": "%s", "datasets": [%s]}', $site_name, implode(',', $datasets));
  
      $results_data[] = '{"pattern": "' . $pattern . '", "siteData": [' . implode(',', $json_texts) . ']}';
    }
  }
 
  $cond_data = array();
  
  if (count($condition_ids) != 0) {
    $cond_pool = implode(',', array_unique($condition_ids));
  
    $query = sprintf('SELECT `id`, `text` FROM `policy_conditions` WHERE `id` IN (%s)', $cond_pool);
  
    $stmt = $history_db->prepare($query);
    $stmt->bind_result($condition_id, $text);
    $stmt->execute();
    while ($stmt->fetch())
      $cond_data[] = sprintf('"%d": "%s"', $condition_id, $text);
    $stmt->close();
  }
  
  echo '{"results":[' . implode(',', $results_data) . '],"conditions":{' . implode(',', $cond_data) . '}}';
  exit(0);  
}
else if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'dumpDeletions') {
  $list = '';

  $query = 'SELECT s.`name`, d.`name`, z.`size` * 1.e-9 FROM `deleted_replicas` AS r';
  $query .= ' INNER JOIN `deletion_requests` AS q ON q.`id` = r.`deletion_id`';
  $query .= ' INNER JOIN `sites` AS s ON s.`id` = q.`site_id`';
  $query .= ' INNER JOIN `datasets` AS d ON d.`id` = r.`dataset_id`';
  $query .= ' INNER JOIN `replica_snapshot_cache` AS c ON (c.`run_id`, c.`site_id`, c.`dataset_id`) = (q.`run_id`, q.`site_id`, r.`dataset_id`)';
  $query .= ' INNER JOIN `replica_size_snapshots` AS z ON z.`id` = c.`size_snapshot_id`';
  $query .= ' WHERE q.`run_id` = ?';
  $query .= ' ORDER BY s.`name`, d.`name`';

  $stmt = $history_db->prepare($query);
  $stmt->bind_param('i', $cycle);
  $stmt->bind_result($site, $dataset, $size);
  $stmt->execute();
  while ($stmt->fetch())
    $list .= $site . "\t" . $dataset . "\t" . sprintf("%.2f", $size) . "\n";
  $stmt->close();

  header('Content-Disposition: attachment; filename="deletions_' . $cycle . '.txt"');
  header('Content-Type: text/plain');
  header('Content-Length: ' . strlen($list));
  header('Connection: close');
  echo $list;
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

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'getData') {
  // main data structure
  $data = array();

  $data['operation'] = $operation;
  $data['cycleNumber'] = $cycle;
  $data['previousCycle'] = $prev_cycle;
  $data['nextCycle'] = $next_cycle;
  $data['cyclePolicy'] = $policy_version;
  $data['comment'] = $comment;
  $data['cycleTimestamp'] = $timestamp;
  $data['partition'] = $partition_id;

  // $timestamp is local time string
  $ts = strptime($timestamp, '%Y-%m-%d %H:%M:%S');
  // converting a local time tuple to unix time
  $unixtime = mktime($ts['tm_hour'], $ts['tm_min'], $ts['tm_sec'], 1 + $ts['tm_mon'], $ts['tm_mday'], 1900 + $ts['tm_year']);
  $data['timestampWarning'] = ($unixtime < mktime() - 3600 * 1); // warn if timestamp is more than 18 hours in the past

  if (isset($_REQUEST['dataType']) && $_REQUEST['dataType'] == 'summary') {
    $index_to_id = array(0);
    $site_names = array(0 => 'Total');
    
    $quotas = array(0 => 0);

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
      $quotas[0] += $quota;
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

    $statuses = array(0 => 1);

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
    foreach ($index_to_id as $id)
      $site_total['protect'][$id] = $site_total['keep'][$id] = $site_total['delete'][$id] = $site_total['protectPrev'][$id] = $site_total['keepPrev'][$id] = 0.;

    $site_total['protect'][0] = $site_total['keep'][0] = $site_total['delete'][0] = $site_total['protectPrev'][0] = $site_total['keepPrev'][0] = 0.;

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
    while ($stmt->fetch()) {
      $site_total[$decision][$site_id] = $size;
      $site_total[$decision][0] += $size;
    }
    $stmt->close();

    check_cache($history_db, $prev_cycle, $partition_id);

    $query = 'SELECT c.`site_id`, CONCAT(l.`decision`, \'Prev\'), SUM(r.`size`) * 1.e-12';
    $query .= ' FROM `replica_snapshot_cache` AS c';
    $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
    $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
    $query .= ' WHERE c.`run_id` = ? AND l.`decision` != \'delete\'';
    $query .= ' GROUP BY c.`site_id`, l.`decision`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $prev_cycle);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch()) {
      $site_total[$decision][$site_id] = $size;
      $site_total[$decision][0] += $size;
    }
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

    $data['requestIds'] = array();

    $stmt = $history_db->prepare('SELECT `id` FROM `deletion_requests` WHERE `run_id` = ? AND `id` > 0');
    $stmt->bind_param('i', $cycle);
    $stmt->bind_result($request_id);
    $stmt->execute();
    while ($stmt->fetch())
      $data['requestIds'][] = $request_id;
    $stmt->close();
  }
  else if (isset($_REQUEST['dataType']) && $_REQUEST['dataType'] == 'siteDetail') {
    $site_name = isset($_REQUEST['siteName']) ? $_REQUEST['siteName'] : '';

    $data['content'] = array('name' => $site_name, 'datasets' => array());
    $datasets = &$data['content']['datasets'];

    $query = 'SELECT d.`name`, r.`size` * 1.e-9, l.`decision`, l.`matched_condition`';
    $query .= ' FROM `replica_snapshot_cache` AS c';
    $query .= ' INNER JOIN `replica_size_snapshots` AS r ON r.`id` = c.`size_snapshot_id`';
    $query .= ' INNER JOIN `deletion_decisions` AS l ON l.`id` = c.`decision_id`';
    $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
    $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
    $query .= ' WHERE c.`run_id` = ? AND s.`name` LIKE ?';
    $query .= ' ORDER BY r.`size` DESC';

    $conditions = array();

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('is', $cycle, $site_name);
    $stmt->bind_result($name, $size, $decision, $condition_id);
    $stmt->execute();
    while ($stmt->fetch()) {
      $datasets[] = array('name' => $name, 'size' => $size, 'decision' => $decision, 'conditionId' => $condition_id);
      $condition_ids[] = $condition_id;
    }
    $stmt->close();

    $data['conditions'] = array();

    $condition_ids = array_unique($condition_ids);

    $query = sprintf('SELECT `id`, `text` FROM `policy_conditions` WHERE `id` IN (%s)', implode(',', $condition_ids));
    $stmt = $history_db->prepare($query);
    $stmt->bind_result($condition_id, $text);
    $stmt->execute();
    while ($stmt->fetch())
      $data['conditions'][$condition_id] = $text;
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
