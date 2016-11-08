<?php
/*
  Included from main. Final product is the array $content.
  Following variables are imported:
  $store_db: mysqi, dynamo inventory store
  $categories: string, what the results will be presented in terms of
  $key_column: key column
  $grouping: grouping by
  $const_*: string, constraints
  $constraints: array of constraint strings
  $physical: bool, show physical/projected size
  $content: output array
*/

/* CONSTRUCT QUERY */

$uses_group = ($categories == 'groups' || count($const_group) != 0);

if ($categories == 'campaigns' || $categories == 'dataTiers' || $categories == 'datasets') {
  $key_column = 'd.`name`';
}
else if ($categories == 'groups') {
  $key_column = 'IFNULL(g.`name`, \'Unsubscribed\')';
}

$query = 'SELECT ' . $key_column . ', COUNT(dr.`site_id`)
FROM `dataset_replicas` AS dr
INNER JOIN `datasets` AS d ON d.`id` = dr.`dataset_id`
INNER JOIN `sites` AS s ON s.`id` = dr.`site_id`';

if ($uses_group) {
  // group is inner-joined on purpose
 
  $query .= ' INNER JOIN (
  SELECT b.`dataset_id` AS dataset_id, br.`site_id` AS site_id, MIN(`groups`.`name`) AS name
  FROM `block_replicas` AS br
  INNER JOIN `groups` ON `groups`.`id` = br.`group_id`
  INNER JOIN `blocks` AS b ON b.`id` = br.`block_id`
  GROUP BY dataset_id, site_id
) AS g ON (g.`dataset_id`, g.`site_id`) = (dr.`dataset_id`, dr.`site_id`)';
}

if ($physical) // full only
  $constraints[] = 'dr.`completion` LIKE \'full\'';

if (count($constraints) != 0)
  $query .= ' WHERE ' . implode(' AND ', $constraints);

$query .= ' GROUP BY dr.`dataset_id`';

/* EXECUTE AND FILL */

$stmt = $store_db->prepare($query);
$stmt->bind_result($name, $repl);
$stmt->execute();

$repl_counts = array();

while ($stmt->fetch()) {
  $key = $categories($name);

  if (array_key_exists($key, $repl_counts))
    $repl_counts[$key][] = $repl;
  else
    $repl_counts[$key] = array($repl);
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
