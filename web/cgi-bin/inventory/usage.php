<?php
/*
  Included from main. Final product is the array $content.
  Following variables are imported:
  $store_db: mysqi, dynamo inventory store
  $categories: string, what the results will be presented in terms of
  $key_column: key column
  $grouping: grouping by
  $constraints: array of constraint strings
  $physical: bool, show physical/projected size
  $content: output array
*/

/* CONSTRUCT QUERY */

$uses_group = ($categories == 'groups' || count($const_group) != 0);

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $key_column = 'd.`name`';
  $grouping = 's.`id`, d.`id`';
}
else if ($categories == 'groups') {
  $key_column = 'IFNULL(g.`name`, \'Unsubscribed\')';
  $grouping = 's.`id`, g.`name`';
}

if ($physical) {
  $query = 'SELECT s.`name`, ' . $key_column . ', SUM(IFNULL(brs.`size`, b.`size`)) * 1.e-12
FROM `block_replicas` AS br
INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`
INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`
INNER JOIN `sites` AS s ON s.`id` = br.`site_id`
LEFT JOIN `groups` AS g ON g.`id` = br.`group_id`
LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (br.`block_id`, br.`site_id`)';
}
else {
  // using MIN(group_name) as the group name of the dataset replica

  $query = 'SELECT s.`name`, ' . $key_column . ', SUM(d.`size`) * 1.e-12
FROM `dataset_replicas` AS dr
INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`
INNER JOIN `sites` AS s ON s.`id` = dr.`site_id`';

  if ($uses_group) {
    $query .= ' INNER JOIN (
  SELECT b.`dataset_id` AS dataset_id, br.`site_id` AS site_id, MIN(`groups`.`name`) AS name
  FROM `block_replicas` AS br
  INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`
  LEFT JOIN `groups` ON `groups`.`id` = br.`group_id`
  GROUP BY dataset_id, site_id
) AS g ON (g.`dataset_id`, g.`site_id`) = (dr.`dataset_id`, dr.`site_id`)';
  }
}

if (count($constraints) != 0)
  $query .= ' WHERE ' . implode(' AND ', $constraints);

$query .= ' GROUP BY ' . $grouping;

$query = str_replace("\n", " ", $query);
error_log($query);

/* EXECUTE AND FILL */

$stmt = $store_db->prepare($query);
$stmt->bind_result($site, $name, $size);
$stmt->execute();

$site_usages = array();
$total_usage = array();

while ($stmt->fetch()) {
  if (!array_key_exists($site, $site_usages))
    $site_usages[$site] = array();

  $usage = &$site_usages[$site];

  $key = $categories($name); // see keys.php

  if (array_key_exists($key, $usage))
    $usage[$key] += $size;
  else
    $usage[$key] = $size;

  if (array_key_exists($key, $total_usage))
    $total_usage[$key] += $size;
  else
    $total_usage[$key] = $size;
}

$stmt->close();

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
