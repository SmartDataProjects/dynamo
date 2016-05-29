<?php

include_once(__DIR__ . '/../common/init_db.php');

$run = 0;
$timestamp = '';
$partition_id = 1;

if (isset($_REQUEST['run_number'])) {
  $run = 0 + $_REQUEST['run_number'];
  $stmt = $db->prepare('SELECT `time_start` FROM `runs` WHERE `id` = ? AND `operation` IN (\'deletion\', \'deletion_test\')');
  $stmt->bind_param('i', $run);
  $stmt->bind_result($timestamp);
  $stmt->execute();
  if ($stmt->num_rows == 0)
    $run = 0;
  else
    $stmt->fetch();
  $stmt->close();
}

if ($run == 0) {
  $stmt = $db->prepare('SELECT `id`, `time_start` FROM `runs` WHERE `operation` IN (\'deletion\', \'deletion_test\') ORDER BY `id` DESC LIMIT 1');
  $stmt->bind_result($run, $timestamp);
  $stmt->execute();
  if ($stmt->num_rows != 0)
    $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['partition_id'])) {
  $partition_id = 0 + $_REQUEST['partition_id'];
  $stmt = $db->prepare('SELECT `name` FROM `partitions` WHERE `id` = ?');
  $stmt->bind_param('i', $partition_id);
  $stmt->bind_result($partition);
  $stmt->execute();
  if ($stmt->num_rows == 0)
    $partition_id = 1;
  else
    $stmt->fetch();
  $stmt->close();
}

if (isset($_REQUEST['data'])) {
  // main data structure
  $data = array();

  $data['run'] = $run;
  $data['partition'] = $partition_id;

  $data['partitions'] = array();
  $stmt = $db->prepare('SELECT `id`, `name` FROM `partitions` ORDER BY `id`');
  $stmt->bind_result($id, $name);
  $stmt->execute();
  while ($stmt->fetch())
    $data['partitions'][] = array('id' => $id, 'name' => $name);
  $stmt->close();

  $data['sites'] = array();
  $stmt = $db->prepare('SELECT `id`, `name` FROM `sites` ORDER BY `name`');
  $stmt->bind_result($id, $name);
  $stmt->execute();
  while ($stmt->fetch())
    $data['sites'][] = array('id' => $id, 'name' => $name);
  $stmt->close();

  $data['quotas'] = array();
  $stmt = $db->prepare('SELECT `site_id`, `quota` FROM `quota_snapshots` WHERE `partition_id` = ? AND `run_id` <= ? ORDER BY `run_id` DESC');
  $stmt->bind_param('ii', $partition_id, $run);
  $stmt->bind_result($site_id, $quota);
  $stmt->execute();
  while ($stmt->fetch()) {
    if (array_key_exists($site_id, $data['quotas']))
      continue;

    $data['quotas'][$site_id] = $quota;

    if (count($data['quotas']) == count($data['sites']))
      break;
  }

  $data['replicas'] = array();
  foreach ($data['sites'] as $site)
    $data['replicas'][$site['id']] = array();
  $stmt = $db->prepare('SELECT s.`site_id`, s.`dataset_id`, s.`size`, d.`decision` FROM `deletion_decisions` AS d INNER JOIN `replica_snapshots` AS s ON s.`id` = d.`snapshot_id` WHERE d.`run_id` = ?');
  $stmt->bind_param('i', $run);
  $stmt->bind_result($site_id, $dataset_id, $size, $decision);
  $stmt->execute();
  while ($stmt->fetch())
    $data['replicas'][$site_id][] = array('ds' => $dataset_id, 'size' => $size, 'dec' => $decision);

  echo json_encode($data);
}
else {
  $html = file_get_contents(__DIR__ . '/html/detox.html');

  $html = str_replace('${RUN_NUMBER}', "$run", $html);
  $html = str_replace('${RUN_TIMESTAMP}', "$timestamp", $html);
  $html = str_replace('${PARTITION}', "$partition_id", $html);

  echo $html;
}

?>
