<?php
/*
  Included from main. Final product is the array $content.
*/

$size_sums = array();

$fetch_size = function($selection, $constraint_base, $grouping) {
  global $store_db;
  global $categories;
  global $const_campaign, $const_data_tier, $const_dataset, $const_site, $const_group;
  global $size_sums;

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
  $stmt->bind_result($name, $size);
  $stmt->execute();

  if ($categories == 'campaigns') {
    while ($stmt->fetch()) {
      preg_match('/^\/[^\/]+\/([^\/-]+)-[^\/]+\/.*/', $name, $matches);
      if (array_key_exists($matches[1], $size_sums))
        $size_sums[$matches[1]] += $size;
      else
        $size_sums[$matches[1]] = $size;
    }
  }
  else if ($categories == 'dataTiers') {
    while ($stmt->fetch()) {
      preg_match('/^\/[^\/]+\/[^\/]+\/([^\/-]+)/', $name, $matches);
      if (array_key_exists($matches[1], $size_sums))
        $size_sums[$matches[1]] += $size;
      else
        $size_sums[$matches[1]] = $size;
    }
  }
  else if ($categories == 'datasets' || $categories == 'sites' || $categories == 'groups') {
    while ($stmt->fetch()) {
      if (array_key_exists($name, $size_sums))
        $size_sums[$name] += $size;
      else
        $size_sums[$name] = $size;
    }
  }

  $stmt->close();
};

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT `datasets`.`name`, SUM(`datasets`.`size`) * 1.e-12 FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  if (strlen($const_site) != 0)
    $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `dataset_replicas`.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';

  $grouping = ' GROUP BY `datasets`.`id`';
}
else if ($categories == 'sites') {
  $selection = 'SELECT `sites`.`name`, SUM(`datasets`.`size`) * 1.e-12 FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `dataset_replicas`.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';

  $grouping = ' GROUP BY `sites`.`id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT `groups`.`name`, SUM(`datasets`.`size`) * 1.e-12 FROM `dataset_replicas`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `dataset_replicas`.`dataset_id`';
  $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `dataset_replicas`.`group_id`';
  if (strlen($const_site) != 0)
    $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `dataset_replicas`.`site_id`';

  $grouping = ' GROUP BY `groups`.`id`';
}

$constraint_base = ' WHERE `dataset_replicas`.`is_complete` = 1 AND `dataset_replicas`.`is_partial` = 0';

$fetch_size($selection, $constraint_base, $grouping);

// block replicas are saved for dataset replicas that are
//  . incomplete or partial
//  . not entirely owned by a group
if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT `datasets`.`name`, SUM(`blocks`.`size`) * 1.e-12 FROM `block_replicas`';
  $selection .= ' INNER JOIN `blocks` ON `blocks`.`id` = `block_replicas`.`block_id`';
  $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id`';
  if (strlen($const_site) != 0)
    $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `block_replicas`.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `block_replicas`.`group_id`';

  $grouping = ' GROUP BY `datasets`.`id`';
}
else if ($categories == 'sites') {
  $selection = 'SELECT `sites`.`name`, SUM(`blocks`.`size`) * 1.e-12 FROM `block_replicas`';
  $selection .= ' INNER JOIN `blocks` ON `blocks`.`id` = `block_replicas`.`block_id`';
  $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `block_replicas`.`site_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset) != 0)
    $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` ON `groups`.`id` = `block_replicas`.`group_id`';

  $grouping = ' GROUP BY `sites`.`id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT `groups`.`name`, SUM(`blocks`.`size`) * 1.e-12 FROM `block_replicas`';
  $selection .= ' INNER JOIN `blocks` ON `blocks`.`id` = `block_replicas`.`block_id`';
  $selection .= ' INNER JOIN `group` ON `groups`.`id` = `block_replicas`.`group_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset) != 0)
    $selection .= ' INNER JOIN `datasets` ON `datasets`.`id` = `blocks`.`dataset_id`';
  if (strlen($const_site) != 0)
    $selection .= ' INNER JOIN `sites` ON `sites`.`id` = `block_replicas`.`site_id`';

  $grouping = ' GROUP BY `groups`.`id`';
}

$constraint_base = ' WHERE `block_replicas`.`is_complete` = 1';

$fetch_size($selection, $constraint_base, $grouping);

arsort($size_sums);
    
foreach ($size_sums as $key => $value)
  $content[] = array('key' => $key, 'size' => $value);

?>
