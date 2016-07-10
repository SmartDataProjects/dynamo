<?php

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT d.`name`, COUNT(dr.`site_id`) FROM `dataset_replicas` AS dr';
  $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` AS g ON g.`id` = dr.`group_id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT g.`name`, COUNT(dr.`site_id`) FROM `dataset_replicas` AS dr';
  $selection .= ' INNER JOIN `groups` AS g ON g.`id` = dr.`group_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset))
    $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`';
}

$grouping = ' GROUP BY dr.`dataset_id`';

$constraints = array();
if ($physical)
  $constraints[] = 'dr.`is_complete` = 1';

if (strlen($const_campaign) != 0)
  $constraints[] = 'd.`name` LIKE \'/%/' . $const_campaign . '%/%\'';
if (strlen($const_data_tier) != 0)
  $constraints[] = 'd.`name` LIKE \'/%/%/' . $const_data_tier . '\'';
if (strlen($const_dataset) != 0)
  $constraints[] = 'd.`name` LIKE \'' . $const_dataset . '\'';
if (count($const_group) != 0) {
  $subconsts = array();
  foreach ($const_group as $cg)
    $subconsts[] = 'g.`name` LIKE \'' . $cg . '\'';
  $constraints[] = '(' . implode(' OR ', $subconsts) . ')';
}

if (count($constraints) != 0)
  $constraint = ' WHERE ' . implode(' AND ', $constraints);
else
  $constraint = '';

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
