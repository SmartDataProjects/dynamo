<?php
/*
  Included from main. Final product is the array $content.
  Following variables are imported:
  $store_db: mysqi, dynamo inventory store
  $categories: string, what the results will be presented in terms of
  $const_*: string, constraints
  $physical: bool, show physical/projected size
  $data: output array
  $content: output array. Part of $data
*/

$site_usages = array();
$total_usage = array();

function fetch_size($selection, $constraint_base, $grouping) {
  global $store_db;
  global $categories;
  global $const_campaign, $const_data_tier, $const_dataset, $const_site, $const_group;
  global $site_usages, $total_usage;

  $constraints = array();

  if ($constraint_base != '')
    $constraints[] = $constraint_base;
  if (strlen($const_campaign) != 0)
    $constraints[] = 'd.`name` LIKE \'/%/' . $const_campaign . '%/%\'';
  if (strlen($const_data_tier) != 0)
    $constraints[] = 'd.`name` LIKE \'/%/%/' . $const_data_tier . '\'';
  if (strlen($const_dataset) != 0)
    $constraints[] = 'd.`name` LIKE \'' . $const_dataset . '\'';
  if (strlen($const_site) != 0)
    $constraints[] = 's.`name` LIKE \'' . $const_site . '\'';
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

  $stmt = $store_db->prepare($selection . ' ' . $constraint . ' ' . $grouping);
  $stmt->bind_result($site, $name, $size);
  $stmt->execute();

  if ($categories == 'campaigns') {
    while ($stmt->fetch()) {
      if (!array_key_exists($site, $site_usages))
        $site_usages[$site] = array();

      $usage = &$site_usages[$site];

      preg_match('/^\/[^\/]+\/(?:(Run20.+)-v[0-9]+|([^\/-]+)-[^\/]+)\/.*/', $name, $matches);
      $key = $matches[2];
      if ($key == "")
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
  $selection = 'SELECT s.`name`, d.`name`, SUM(d.`size`) * 1.e-12 FROM `dataset_replicas` AS dr';
  $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`';
  $selection .= ' INNER JOIN `sites` AS s ON s.`id` = dr.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` AS g ON g.`id` = dr.`group_id`';

  $grouping = ' GROUP BY s.`id`, d.`id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT s.`name`, g.`name`, SUM(d.`size`) * 1.e-12 FROM `dataset_replicas` AS dr';
  $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`';
  $selection .= ' INNER JOIN `groups` AS g ON g.`id` = dr.`group_id`';
  $selection .= ' INNER JOIN `sites` AS s ON s.`id` = dr.`site_id`';

  $grouping = ' GROUP BY s.`id`, g.`id`';
}

$constraint_base = 'dr.`is_complete` = 1 AND dr.`is_partial` = 0';

fetch_size($selection, $constraint_base, $grouping);

// block replicas are saved for dataset replicas that are
//  . incomplete or partial
//  . not entirely owned by a group
if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $selection = 'SELECT s.`name`, d.`name`, SUM(b.`size`) * 1.e-12 FROM `block_replicas` AS br';
  $selection .= ' INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`';
  $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`';
  $selection .= ' INNER JOIN `sites` AS s ON s.`id` = br.`site_id`';
  if (count($const_group) != 0)
    $selection .= ' INNER JOIN `groups` AS g ON g.`id` = br.`group_id`';

  $grouping = ' GROUP BY s.`id`, d.`id`';
}
else if ($categories == 'groups') {
  $selection = 'SELECT s.`name`, g.`name`, SUM(b.`size`) * 1.e-12 FROM `block_replicas` AS br';
  $selection .= ' INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`';
  $selection .= ' INNER JOIN `groups` AS g ON g.`id` = br.`group_id`';
  $selection .= ' INNER JOIN `sites` AS s ON s.`id` = br.`site_id`';
  if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset) != 0)
    $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`';

  $grouping = ' GROUP BY s.`id`, g.`id`';
}

if ($physical) # only pick up the block full sizes for complete replicas
  $constraint_base = 'br.`is_complete` = 1';
else
  $constraint_base = '';

fetch_size($selection, $constraint_base, $grouping);

if ($physical) {
# now tally up the incomplete replicas

  if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
    $selection = 'SELECT s.`name`, d.`name`, SUM(brs.`size`) * 1.e-12 FROM `block_replica_sizes` AS brs';
    $selection .= ' INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id`';
    $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`';
    $selection .= ' INNER JOIN `sites` AS s ON s.`id` = brs.`site_id`';
    if (count($const_group) != 0) {
      $selection .= ' INNER JOIN `block_replicas` AS br ON br.`block_id` = brs.`block_id` AND br.`site_id` = brs.`site_id`';
      $selection .= ' INNER JOIN `groups` AS g ON g.`id` = br.`group_id`';
    }

    $grouping = ' GROUP BY s.`id`, d.`id`';
  }
  else if ($categories == 'groups') {
    $selection = 'SELECT s.`name`, g.`name`, SUM(brs.`size`) * 1.e-12 FROM `block_replica_sizes` AS brs';
    $selection .= ' INNER JOIN `block_replicas` AS br ON br.`block_id` = brs.`block_id` AND br.`site_id` = brs.`site_id`';
    $selection .= ' INNER JOIN `groups` AS g ON g.`id` = br.`group_id`';
    $selection .= ' INNER JOIN `sites` AS s ON s.`id` = brs.`site_id`';
    if (strlen($const_campaign) != 0 || strlen($const_data_tier) != 0 || strlen($const_dataset) != 0) {
      $selection .= ' INNER JOIN `blocks` AS b ON b.`id` = brs.`block_id`';
      $selection .= ' INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`';
    }

    $grouping = ' GROUP BY s.`id`, g.`id`';
  }

  fetch_size($selection, '', $grouping);
}

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
