<?php

include_once(__DIR__ . '/../common/db_conf.php');
include(__DIR__ . '/check_cache.php');

date_default_timezone_set('America/New_York');

$history_db_name = 'dynamohistory';
$cache_db_name = $history_db_name . '_cache';

$history_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $history_db_name);

$operation = 'deletion';

if (isset($TESTMODE) && $TESTMODE)
  $operation = 'deletion_test';

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'getPartitions') {
  $data = array();
  
  $stmt = $history_db->prepare('SELECT DISTINCT `partitions`.`id`, `partitions`.`name` FROM `runs` INNER JOIN `partitions` ON `partitions`.`id` = `runs`.`partition_id` WHERE `runs`.`operation` = ? ORDER BY `partitions`.`id`');
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
  $stmt = $history_db->prepare('SELECT `partition_id`, `policy_version`, `comment`, `time_start` FROM `runs` WHERE `id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = ?');
  $stmt->bind_param('is', $cycle, $operation);
  $stmt->bind_result($partition_id, $policy_version, $comment, $timestamp);
  $stmt->execute();
  if (!$stmt->fetch())
    $cycle = 0;
  $stmt->close();
}
else if (isset($_REQUEST['partition'])) {
  $stmt = $history_db->prepare('SELECT `id` FROM `partitions` WHERE `name` = ?');
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

  $stmt = $history_db->prepare('SELECT `id`, `policy_version`, `comment`, `time_start` FROM `runs` WHERE `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = ? ORDER BY `id` DESC LIMIT 1');
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

if (!check_cache($history_db, $cycle, $partition_id, $snapshot_spool_path, $snapshot_archive_path)) {
  // we should emit some error here
  exit(1);
}

$replica_cache_table_name = sprintf('`%s`.`replicas_%d`', $cache_db_name, $cycle);
$site_cache_table_name = sprintf('`%s`.`sites_%d`', $cache_db_name, $cycle);

if (isset($_REQUEST['command']) && $_REQUEST['command'] == 'searchDataset') {
  // return
  // {"results": [{"siteData": [{"name": site, "datasets": [datasets]}]}], "conditions": [conditions]}

  $results_data = array();

  if (isset($_REQUEST['datasetNames']) && is_array($_REQUEST['datasetNames'])) {
    $query = 'SELECT s.`id`, s.`name`, d.`name`, c.`size` * 1.e-9, c.`decision`, c.`condition`, t.`count`';
    $query .= ' FROM ' . $replica_cache_table_name . ' AS c';
    $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
    $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
    $query .= ' INNER JOIN (SELECT `site_id`, `dataset_id`, COUNT(*) AS count FROM ' . $replica_cache_table_name . ' GROUP BY `site_id`, `dataset_id`) AS t';
    $query .= '  ON t.`site_id` = c.`site_id` AND t.`dataset_id` = c.`dataset_id`';
    $query .= ' WHERE d.`name` LIKE ?';
    $query .= ' ORDER BY s.`id`, d.`id`';

    foreach ($_REQUEST['datasetNames'] as $pattern) {
      $dataset_pattern = str_replace('_', '\_', $pattern);
      $dataset_pattern = str_replace('*', '%', $dataset_pattern);
      $dataset_pattern = str_replace('?', '_', $dataset_pattern);

      $stmt = $history_db->prepare($query);
      $stmt->bind_param('s', $dataset_pattern);
      $stmt->bind_result($sid, $site_name, $dataset_name, $size, $decision, $condition_id, $count);
      $stmt->execute();

      $condition_ids = array();

      $json_data = array();

      $protect_total = 0.;
      $keep_total = 0.;
      $delete_total = 0.;

      $_sid = 0;
      while ($stmt->fetch()) {
        if ($sid != $_sid) {
          $json_data[$site_name] = array();
          $_sid = $sid;
        }

        if ($decision == 'protect')
          $protect_total += $size;
        else if ($decision == 'keep')
          $keep_total += $size;
        else
          $delete_total += $size;

        if ($count != 1)
          $decision .= ' *';
  
        $json_data[$site_name][] = sprintf('{"name":"%s","size":%f,"decision":"%s","conditionId":"%s"}', $dataset_name, $size, $decision, $condition_id);
        $condition_ids[] = $condition_id;
      }
      $stmt->close();
  
      $json_texts = array();
      // for Total as a site, we have a different format: just report total size
      $json_texts[] = sprintf('{"name":"Total","protect":%f,"keep":%f,"delete":%f}', $protect_total, $keep_total, $delete_total);
      foreach ($json_data as $site_name => $datasets)
        $json_texts[] = sprintf('{"name":"%s","datasets":[%s]}', $site_name, implode(',', $datasets));
  
      $results_data[] = '{"pattern":"' . $pattern . '","siteData":[' . implode(',', $json_texts) . ']}';
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

  $query = 'SELECT s.`name`, d.`name`, c.`size` * 1.e-9 FROM `deleted_replicas` AS r';
  $query .= ' INNER JOIN `deletion_requests` AS q ON q.`id` = r.`deletion_id`';
  $query .= ' INNER JOIN `sites` AS s ON s.`id` = q.`site_id`';
  $query .= ' INNER JOIN `datasets` AS d ON d.`id` = r.`dataset_id`';
  $query .= ' INNER JOIN ' . $replica_cache_table_name . ' AS c ON (c.`site_id`, c.`dataset_id`) = (q.`site_id`, r.`dataset_id`)';
  $query .= ' WHERE q.`run_id` = ? AND c.`decision` = \'delete\'';
  // A BAD HACK
  if ($partition_id != 9)
    $query .= ' AND s.`name` NOT LIKE "%_MSS"';
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

$stmt = $history_db->prepare('SELECT `id` FROM `runs` WHERE `id` > ? AND `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = ? ORDER BY `id` ASC LIMIT 1');
$stmt->bind_param('iis', $cycle, $partition_id, $operation);
$stmt->bind_result($next_cycle);
$stmt->execute();
if (!$stmt->fetch())
  $next_cycle = 0;
$stmt->close();

$stmt = $history_db->prepare('SELECT `id` FROM `runs` WHERE `id` < ? AND `partition_id` = ? AND `time_end` NOT LIKE \'0000-00-00 00:00:00\' AND `operation` = ? ORDER BY `id` DESC LIMIT 1');
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

  if ($next_cycle == 0) {
    // we are showing the latest cycle
    // $timestamp is local time string
    $ts = strptime($timestamp, '%Y-%m-%d %H:%M:%S');
    // converting a local time tuple to unix time
    $unixtime = mktime($ts['tm_hour'], $ts['tm_min'], $ts['tm_sec'], 1 + $ts['tm_mon'], $ts['tm_mday'], 1900 + $ts['tm_year']);
    $data['timestampWarning'] = ($unixtime < mktime() - 3600 * 18); // warn if timestamp is more than 18 hours in the past
  }
  else
    $data['timestampWarning'] = false;

  if (isset($_REQUEST['dataType']) && $_REQUEST['dataType'] == 'summary') {
    $data['siteData'] = array();
    $data['siteData'][] = array('name' => 'Total', 'quota' => 0., 'status' => 1, 'protect' => 0., 'keep' => 0., 'delete' => 0., 'protectPrev' => 0., 'keepPrev' => 0.);

    $id_to_site = array();

    $query = 'SELECT s.`id`, s.`name`, c.`status`, c.`quota` FROM ' . $site_cache_table_name . ' AS c';
    $query .= ' INNER JOIN `sites` AS s ON s.`id` = c.`site_id`';
    if ($partition_id != 9)
      $query.= ' WHERE s.`name` NOT LIKE "%_MSS"';
    $query .= ' ORDER BY s.`name`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_result($site_id, $site_name, $status, $quota);
    $stmt->execute();
    while ($stmt->fetch()) {
      $id_to_site[$site_id] = count($data['siteData']);

      if ($status == 'morgue' || $status == 'waitroom')
        $status_bit = 0;
      else
        $status_bit = 1;

      $data['siteData'][] = array(
        'name' => $site_name,
        'quota' => $quota,
        'status' => $status_bit,
        'protect' => 0.,
        'keep' => 0.,
        'delete' => 0.,
        'protectPrev' => 0.,
        'keepPrev' => 0.
      );

      $data['siteData'][0]['quota'] += $quota;
    }
    $stmt->close();

    $query = 'SELECT `site_id`, `decision`, SUM(`size`) * 1.e-12';
    $query .= ' FROM ' . $replica_cache_table_name . '';
    $query .= ' GROUP BY `site_id`, `decision`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_result($site_id, $decision, $size);
    $stmt->execute();
    while ($stmt->fetch()) {
      if (!array_key_exists($site_id, $id_to_site))
        continue;

      $idx = $id_to_site[$site_id];
      $data['siteData'][$idx][$decision] = $size;
      $data['siteData'][0][$decision] += $size;
    }
    $stmt->close();

    if (check_cache($history_db, $prev_cycle, $partition_id, $snapshot_spool_path, $snapshot_archive_path)) {
      $prev_replica_cache_table_name = sprintf('`%s`.`replicas_%d`', $cache_db_name, $prev_cycle);

      $query = 'SELECT `site_id`, CONCAT(`decision`, \'Prev\'), SUM(`size`) * 1.e-12';
      $query .= ' FROM ' . $prev_replica_cache_table_name . '';
      $query .= ' WHERE `decision` != \'delete\'';
      $query .= ' GROUP BY `site_id`, `decision`';

      $stmt = $history_db->prepare($query);
      $stmt->bind_result($site_id, $decision, $size);
      $stmt->execute();
      while ($stmt->fetch()) {
        if (!array_key_exists($site_id, $id_to_site))
          continue;

        $idx = $id_to_site[$site_id];
        $data['siteData'][$idx][$decision] = $size;
        $data['siteData'][0][$decision] += $size;
      }
      $stmt->close();
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

    $data['conditions'] = array();

    $site_id = 0;
    $query = 'SELECT `id` FROM `sites` WHERE `name` = ?';
    $stmt = $history_db->prepare($query);
    $stmt->bind_param('s', $site_name);
    $stmt->bind_result($site_id);
    $stmt->execute();
    $stmt->fetch();
    $stmt->close();
    if ($site_id == 0) {
      echo json_encode($data);
      exit(1);
    }

    // first collect info about multi-action items
    $multi_action_datasets = array();

    $query = 'SELECT c.`dataset_id`, COUNT(*) FROM ' . $replica_cache_table_name . ' AS c';
    $query .= ' WHERE c.`site_id` = ? GROUP BY c.`dataset_id`';

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $site_id);
    $stmt->bind_result($did, $count);
    $stmt->execute();
    while ($stmt->fetch()) {
      if ($count != 1)
        $multi_action_datasets[] = $did;
    }
    $stmt->close();

    error_log(print_r($multi_action_datasets, true));

    $query = 'SELECT d.`id`, d.`name`, c.`size` * 1.e-9, c.`decision`, c.`condition`';
    $query .= ' FROM ' . $replica_cache_table_name . ' AS c';
    $query .= ' INNER JOIN `datasets` AS d ON d.`id` = c.`dataset_id`';
    $query .= ' WHERE c.`site_id` = ?';
    $query .= ' ORDER BY c.`size` DESC';

    $conditions = array();

    $stmt = $history_db->prepare($query);
    $stmt->bind_param('i', $site_id);
    $stmt->bind_result($did, $name, $size, $decision, $condition_id);
    $stmt->execute();
    while ($stmt->fetch()) {
      if (in_array($did, $multi_action_datasets))
        $decision .= " *";
      
      $datasets[] = array('name' => $name, 'size' => $size, 'decision' => $decision, 'conditionId' => $condition_id);
      $condition_ids[] = $condition_id;
    }
    $stmt->close();

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
