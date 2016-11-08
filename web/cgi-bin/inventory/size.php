<?php
/*
  Included from main. Final product is the array $content.
  Following variables are imported:
  $store_db: mysqi, dynamo inventory store
  $categories: string, what the results will be presented in terms of
  $constraints: array of constraint strings
  $physical: bool, show physical/projected size
  $content: output array
*/

/* CONSTRUCT QUERIES */

$uses_group = ($categories == 'groups' || count($const_group) != 0);

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $key_column = 'd.`name`';
  $grouping = 'd.`id`';
}
else if ($categories == 'sites') {
  $key_column = 's.`name`';
  $grouping = 's.`id`';
}
else if ($categories == 'groups') {
  $key_column = 'IFNULL(g.`name`, \'Unsubscribed\')';
  $grouping = 'g.`name`';
}

if ($physical) {
  $query = 'SELECT ' . $key_column . ', SUM(IFNULL(brs.`size`, b.`size`)) * 1.e-12
FROM `block_replicas` AS br
INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`
INNER JOIN `datasets` AS d ON d.`id` = b.`dataset_id`
INNER JOIN `sites` AS s ON s.`id` = br.`site_id`
LEFT JOIN `groups` AS g ON g.`id` = br.`group_id`
LEFT JOIN `block_replica_sizes` AS brs ON (brs.`block_id`, brs.`site_id`) = (br.`block_id`, br.`site_id`)';
}
else {
  // using MIN(group_name) as the group name of the dataset replica

  $query = 'SELECT ' . $key_column . ', SUM(d.`size`) * 1.e-12
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

/* EXECUTE AND FILL */

$stmt = $store_db->prepare($query);
$stmt->bind_result($name, $size);
$stmt->execute();

$size_sums = array();

while ($stmt->fetch()) {
  $key = $categories($name); // see keys.php

  if (array_key_exists($key, $size_sums))
    $size_sums[$key] += $size;
  else
    $size_sums[$key] = $size;
}

$stmt->close();

arsort($size_sums);
    
foreach ($size_sums as $key => $value)
  $content[] = array('key' => $key, 'size' => $value);

?>
