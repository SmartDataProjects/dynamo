<?php

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT `datasets`.`name`, COUNT(`dataset_replicas`.`site_id`) FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT `groups`.`name`, COUNT(`dataset_replicas`.`site_id`) FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset))
    $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
}

$grouping = ' GROUP BY `dataset_replicas`.`dataset_id`';

$constraint = ' WHERE `dataset_replicas`.`is_complete` = 1';

if (strlen($const_campaign) != 0)
  $constraint .= ' AND `datasets`.`name` LIKE \'/%/' . $const_campaign . '%/%\'';
if (strlen($const_data_tier) != 0)
  $constraint .= ' AND `datasets`.`name` LIKE \'/%/%/' . $const_data_tier . '\'';
if (strlen($const_dataset) != 0)
  $constraint .= ' AND `datasets`.`name` LIKE \'' . $const_dataset . '\'';
if (count($const_group) != 0) {
  $subconsts = array();
  foreach ($const_group as $cg)
    $subconsts[] = '`groups`.`name` LIKE \'' . $cg . '\'';
  $constraint .= ' AND (' . implode(' OR ', $subconsts) . ')';
}

$stmt = $store_db->prepare($selection . $constraint . $grouping);
$stmt->bind_result($name, $repl);
$stmt->execute();

$repl_counts = array();

if ($categories == 'campaigns') {
  while ($stmt->fetch()) {
    preg_match('/^\/[^\/]+\/(?:(Run20.+)-v[0-9]+|([^\/-]+)-[^\/]+)\/.*/', $name, $matches);
    $key = $matches[2];
    if ($key == "")
      $key = $matches[1];

    if (array_key_exists($key, $repl_counts))
      $repl_counts[$key][] = $repl;
    else
      $repl_counts[$key] = array($repl);
  }
}
else if ($categories == 'dataTiers') {
  while ($stmt->fetch()) {
    preg_match('/^\/[^\/]+\/[^\/]+\/([^\/-]+)/', $name, $matches);
    $key = $matches[1];
    if (array_key_exists($key, $repl_counts))
      $repl_counts[$key][] = $repl;
    else
      $repl_counts[$key] = array($repl);
  }
}
else if ($categories == 'datasets' || $categories == 'groups') {
  while ($stmt->fetch()) {
    $key = $name;
    if (array_key_exists($key, $repl_counts))
      $repl_counts[$key][] = $repl;
    else
      $repl_counts[$key] = array($repl);
  }
}

$stmt->close();

$mean_array = array(); // array of means
$rms_array = array(); // array of rmses

foreach ($repl_counts as $key => $array) {
  if (count($array) == 0) {
    $mean_array[$key] = 0.;
    $rms_array[$key] = 0.;
    continue;
  }
      
  $sumw = 0.;
  $sumw2 = 0.;
  foreach ($array as $count) {
    $sumw += $count;
    $sumw2 += $count * $count;
  }
  $mean = $sumw / count($array);
  $rms = sqrt($sumw2 / count($array) - $mean * $mean);

  $mean_array[$key] = $mean;
  $rms_array[$key] = $rms;
}

arsort($mean_array);

foreach ($mean_array as $key => $mean)
  $content[] = array('key' => $key, 'mean' => $mean, 'rms' => $rms_array[$key]);

?>
