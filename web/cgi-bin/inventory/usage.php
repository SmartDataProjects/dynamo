<?php

$site_usages = array();
$total_usage = array();

$fetch_size = function($selection, $constraint_base, $grouping) {
  global $store_db;
  global $categories;
  global $const_campaign, $const_data_tier, $const_dataset, $const_site, $const_group;
  global $site_usages, $total_usage;

  $constraint = $constraint_base;

  if (strlen($const_campaign) != 0)
    $constraint .= ' AND `datasets`.`name` LIKE \'/%/' . $const_campaign . '%/%\'';
  if (strlen($const_data_tier) != 0)
    $constraint .= ' AND `datasets`.`name` LIKE \'/%/%/' . $const_data_tier . '\'';
  if (strlen($const_dataset) != 0)
    $constraint .= ' AND `datasets`.`name` LIKE \'' . $const_dataset . '\'';
  if (strlen($const_site) != 0)
    $constraint .= ' AND `sites`.`name` LIKE \'' . $const_site . '\'';
  if (count($const_group) != 0) {
    $subconsts = array();
    foreach ($const_group as $cg)
      $subconsts[] = '`groups`.`name` LIKE \'' . $cg . '\'';
    $constraint .= ' AND (' . implode(' OR ', $subconsts) . ')';
  }

  $stmt = $store_db->prepare($selection . $constraint . $grouping);
  $stmt->bind_result($site, $name, $size);
  $stmt->execute();

  if ($categories == 'campaigns') {
    while ($stmt->fetch()) {
      if (!array_key_exists($site, $site_usages))
        $site_usages[$site] = array();

      $usage = &$site_usages[$site];

      preg_match('/^\/[^\/]+\/([^\/-]+)-[^\/]+\/.*/', $name, $matches);
      $key = $matches[1];

      if (array_key_exists($key, $usage))
        $usage[$key] += $size;
      else
        $usage[$key] = $size;

      if (array_key_exists($key, $total_usage))
        $total_usage[$key] += $size;
      else
        $total_usage[$key] = $size;
    }
  }
  else if ($categories == 'dataTiers') {
    while ($stmt->fetch()) {
      if (!array_key_exists($site, $site_usages))
        $site_usages[$site] = array();

      $usage = &$site_usages[$site];

      preg_match('/^\/[^\/]+\/[^\/]+\/([^\/-]+)/', $name, $matches);
      $key = $matches[1];

      if (array_key_exists($key, $usage))
        $usage[$key] += $size;
      else
        $usage[$key] = $size;

      if (array_key_exists($key, $total_usage))
        $total_usage[$key] += $size;
      else
        $total_usage[$key] = $size;
    }
  }
  else if ($categories == 'datasets' || $categories == 'groups') {
    while ($stmt->fetch()) {
      if (!array_key_exists($site, $site_usages))
        $site_usages[$site] = array();

      $usage = &$site_usages[$site];

      $key = $name;

      if (array_key_exists($key, $usage))
        $usage[$key] += $size;
      else
        $usage[$key] = $size;

      if (array_key_exists($key, $total_usage))
        $total_usage[$key] += $size;
      else
        $total_usage[$key] = $size;
    }
  }

  $stmt->close();
};

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT `sites`.`name`, `datasets`.`name`, SUM(`datasets`.`size`) * 1.e-12 FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `dataset_replicas`.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT `sites`.`name`, `groups`.`name`, SUM(`datasets`.`size`) * 1.e-12 FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `dataset_replicas`.`site_id`';
}

$grouping = ' GROUP BY `datasets`.`id`';
$constraint_base = ' WHERE `dataset_replicas`.`is_complete` = 1 AND `dataset_replicas`.`is_partial` = 0';

$fetch_size($selection, $constraint_base, $grouping);

// block replicas are saved for dataset replicas that are
//  . incomplete or partial
//  . not entirely owned by a group
if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT `sites`.`name`, `datasets`.`name`, SUM(`blocks`.`size`) * 1.e-12 FROM `block_replicas`';
  $selection .= ' INNER JOIN `blocks` ON `blocks`.`id` = `block_replicas`.`block_id`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `block_replicas`.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `block_replicas`.`group_id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT `sites`.`name`, `groups`.`name`, SUM(`blocks`.`size`) * 1.e-12 FROM `block_replicas`';
  $selection .= ' INNER JOIN `blocks` ON `blocks`.`id` = `block_replicas`.`block_id`';
  $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `block_replicas`.`group_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `block_replicas`.`site_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset) != 0)
    $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id`';
}

$grouping = ' GROUP BY `blocks`.`id`';
$constraint_base = ' WHERE `block_replicas`.`is_complete` = 1';

$fetch_size($selection, $constraint_base, $grouping);

ksort($site_usages);
arsort($total_usage);

foreach ($site_usages as $site => $usage) {
  $site_content = array();
 
  foreach ($total_usage as $key => $total) {
    if (array_key_exists($key, $usage))
      $site_content[] = array('key' => $key, 'size' => $usage[$key]);
  }

  $content[] = array('site' => $site, 'usage' => $site_content);
}

$data['keys'] = array_keys($total_usage);

?>
