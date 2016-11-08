<?php

include_once(__DIR__ . '/../common/init_db.php');
include_once(__DIR__ . '/../common/utils.php');
include_once(__DIR__ . '/../inventory/keys.php');

$stmt = $store_db->prepare('SELECT `lock_host` FROM `system`');
$stmt->bind_result($lock_host);
$stmt->execute();
$stmt->fetch();
$stmt->close();

if ($lock_host != '') {
  // use the latest snapshot if the inventory is locked
  $result = $store_db->query('SHOW DATABASES');
  $latest = 0;
  while ($row = $result->fetch_row()) {
    $db_name = $row[0];
    if (strpos($db_name, $store_db_name) === false)
      continue;

    $timestamp = 0 + str_replace($store_db_name . '_', '', $db_name);
    if ($timestamp > $latest)
      $latest = $timestamp;
  }
  if ($latest != 0) {
    $store_db->close();
    $store_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $store_db_name . '_' . $latest);
  }
}

if (isset($_REQUEST['getGroups']) && $_REQUEST['getGroups']) {
  $data = array();

  $stmt = $store_db->prepare('SELECT `name` FROM `groups` ORDER BY `name`');
  $stmt->bind_result($name);
  $stmt->execute();
  while ($stmt->fetch())
    $data[] = $name;
  $stmt->close();

  echo json_encode($data);
}
else if (isset($_REQUEST['getData']) && $_REQUEST['getData']) {
  $const_campaign = str_replace('*', '%', $_REQUEST['campaign']);
  $const_data_tier = str_replace('*', '%', $_REQUEST['dataTier']);
  $const_dataset = str_replace('*', '%', $_REQUEST['dataset']);
  $const_site = str_replace('*', '%', $_REQUEST['site']);
  $const_group = array();
  if (isset($_REQUEST['group'])) {
    $groups = array();
    if (is_array($_REQUEST['group']))
      $groups = $_REQUEST['group'];
    else if (strlen($_REQUEST['group']) != 0)
      $groups = explode(',', $_REQUEST['group']);

    foreach($groups as $group)
      $const_group[] = str_replace('*', '%', $group);
  }

  $data_type = $_REQUEST['dataType'];
  $categories = $_REQUEST['categories'];
  $physical = $_REQUEST['physical'] == 'y';

  $constraints = array();

  $constraints[] = 's.`storage_type` NOT LIKE \'mss\'';
  if (strlen($const_site) != 0)
    $constraints[] = 's.`name` LIKE \'' . $const_site . '\'';
  if (count($const_group) != 0)
    $constraints[] = 'g.`name` IN (' . implode(',', array_quote($const_group)) . ')';
  if (strlen($const_campaign) != 0)
    $constraints[] = 'd.`name` LIKE \'/%/' . $const_campaign . '-%/%\'';
  if (strlen($const_data_tier) != 0)
    $constraints[] = 'd.`name` LIKE \'/%/%/' . $const_data_tier . '\'';
  if (strlen($const_dataset) != 0)
    $constraints[] = 'd.`name` LIKE \'' . $const_dataset . '\'';

  $data = array('dataType' => $data_type, 'content' => array());

  $stmt = $store_db->prepare('SELECT `last_update` FROM `system`');
  $stmt->bind_result($last_update);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  $data['lastUpdate'] = $last_update;

  $content = &$data['content'];

  if ($data_type == 'size') {
    include('size.php');
  }
  else if ($data_type == 'replication') {
    include('replication.php');
  }
  else if ($data_type == 'usage') {
    include('usage.php');
  }

  echo json_encode($data);
}
else {
  if (isset($_REQUEST['dataType']))
    $data_type = $_REQUEST['dataType'];
  else
    $data_type = 'size';

  if (isset($_REQUEST['categories']))
    $categories = $_REQUEST['categories'];
  else
    $categories = 'campaigns';

  $constraints = array();
  if (isset($_REQUEST['campaign']))
    $constraints['campaign'] = $_REQUEST['campaign'];
  if (isset($_REQUEST['dataTier']))
    $constraints['dataTier'] = $_REQUEST['dataTier'];
  if (isset($_REQUEST['dataset']))
    $constraints['dataset'] = $_REQUEST['dataset'];
  if (isset($_REQUEST['site']))
    $constraints['site'] = $_REQUEST['site'];
  if (isset($_REQUEST['group'])) {
    if (is_array($_REQUEST['group']))
      $constraints['group'] = $_REQUEST['group'];
    else if (strlen($_REQUEST['group']) != 0)
      $constraints['group'] = explode(',', $_REQUEST['group']);
  }
  if (isset($_REQUEST['physical'])) {
    if ($_REQUEST['physical'] == 'y') {
      $physical_checked = ' checked="checked"';
      $projected_checked = '';
    }
    else {
      $physical_checked = '';
      $projected_checked = ' checked="checked"';
    }
  }
  else {
    $physical_checked = ' checked="checked"';
    $projected_checked = '';
  }

  if (count($constraints) == 0)
    $constraints['group'] = array('AnalysisOps');

  $html = file_get_contents(__DIR__ . '/html/inventory.html');

  $html = str_replace('${DATA_TYPE}', $data_type, $html);
  $html = str_replace('${CATEGORIES}', $categories, $html);
  $html = str_replace('${CONSTRAINTS}', json_encode($constraints), $html);
  $html = str_replace('${PHYSICAL_CHECKED}', $physical_checked, $html);
  $html = str_replace('${PROJECTED_CHECKED}', $projected_checked, $html);

  echo $html;
}

?>