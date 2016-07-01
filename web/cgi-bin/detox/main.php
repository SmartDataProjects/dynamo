<?php

include_once(__DIR__ . '/../common/init_db.php');

if (isset($_REQUEST['getPartitions']) && $_REQUEST['getPartitions']) {
  $data = array();
  
  $stmt = $history_db->prepare('SELECT `id`, `name` FROM `partitions` ORDER BY `id`');
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
$partition_id = 1;

if (isset($_REQUEST['partitionId'])) {
  $partition_id = 0 + $_REQUEST['partitionId'];
  $stmt = $history_db->prepare('SELECT `name` FROM `partitions` WHERE `id` = ?');
  $stmt->bind_param('i', $partition_id);
  $stmt->bind_result($partition);
  $stmt->execute();
  if (!$stmt->fetch())
    $partition_id = 1;
  $stmt->close();
}

if (isset($_REQUEST['cycleNumber'])) {
  $cycle = 0 + $_REQUEST['cycleNumber'];
  $stmt = $history_db->prepare('SELECT `time_start` FROM `runs` WHERE `id` = ? AND `partition_id` = ? AND `operation` IN (\'deletion\', \'deletion_test\')');
  $stmt->bind_param('ii', $cycle, $partition_id);
  $stmt->bind_result($timestamp);
  $stmt->execute();
  if (!$stmt->fetch())
    $cycle = 0;
  $stmt->close();
}

if ($cycle == 0) {
  $stmt = $history_db->prepare('SELECT `id`, `time_start` FROM `runs` WHERE `partition_id` = ? AND `operation` IN (\'deletion\', \'deletion_test\') ORDER BY `id` DESC LIMIT 1');
  $stmt->bind_param('i', $partition_id);
  $stmt->bind_result($cycle, $timestamp);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['getData']) && $_REQUEST['getData']) {
  // main data structure
  $data = array();

  $data['cycleNumber'] = $cycle;
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

    foreach (array('protect', 'keep', 'delete') as $decision) {
      ${$decision} = array();

      $stmt = $history_db->prepare('SELECT s.`site_id`, SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? AND d.`decision` LIKE ? GROUP BY s.`site_id`');
      $stmt->bind_param('is', $cycle, $decision);
      $stmt->bind_result($site_id, $size);
      $stmt->execute();
      while ($stmt->fetch())
        ${$decision}[$site_id] = $size;
      $stmt->close();
    }

    $previous_cycle = 0;
    $stmt = $history_db->prepare('SELECT MAX(`id`) FROM `runs` WHERE `partition_id` = ? AND `operation` IN (\'deletion\', \'deletion_test\') AND `id` < ?');
    $stmt->bind_param('ii', $partition_id, $cycle);
    $stmt->bind_result($previous_cycle);
    $stmt->execute();
    $stmt->fetch();
    $stmt->close();

    foreach (array('protectPrev', 'keepPrev') as $decision) {
      ${$decision} = array();

      $stmt = $history_db->prepare('SELECT s.`site_id`, SUM(s.`size`) * 1.e-12 FROM `deletion_decisions` AS d INNER JOIN `replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ? AND d.`decision` LIKE ? GROUP BY s.`site_id`');
      $stmt->bind_param('is', $previous_cycle, str_replace('Prev', '', $decision));
      $stmt->bind_result($site_id, $size);
      $stmt->execute();
      while ($stmt->fetch())
        ${$decision}[$site_id] = $size;
      $stmt->close();
    }

    $data['siteData'] = array();
    foreach ($index_to_id as $id) {
      $data['siteData'][] =
        array(
              'id' => $id,
              'name' => $site_names[$id],
              'quota' => $quotas[$id],
              'protect' => 0. + $protect[$id],
              'keep' => 0. + $keep[$id],
              'delete' => 0. + $delete[$id],
              'protectPrev' => 0. + $protectPrev[$id],
              'keepPrev' => 0. + $keepPrev[$id]
              );
    }
  }
  else if ($_REQUEST['dataType'] == 'siteDetail') {
    $site_name = $_REQUEST['siteName'];

    $data['name'] = $site_name;

    $data['datasets'] = array();

    $query = 'SELECT ds.`name`, sn.`size` * 1.e-9, dl.`decision`, dl.`reason` FROM `deletion_decisions` AS dl';
    $query .= ' INNER JOIN `replica_snapshots` AS sn ON sn.`id` = dl.`snapshot_id`';
    $query .= ' INNER JOIN `datasets` AS ds ON ds.`id` = sn.`dataset_id`';
    $query .= ' INNER JOIN `sites` AS st ON st.`id` = sn.`site_id`';
    $query .= ' WHERE dl.`run_id` = ? AND st.`name` LIKE ? AND dl.`decision` IN (\'delete\', \'protect\')';
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
