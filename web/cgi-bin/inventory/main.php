<?php

include_once(__DIR__ . '/../common/init_db.php');

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
  foreach (array('campaign', 'dataTier', 'dataset', 'site') as $const) {
    if (isset($_REQUEST[$const]))
      ${'const_' . $const} = str_replace('*', '%', $_REQUEST[$const]);
    else
      ${'const_' . $const} = '';
  }
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

  $data = array('dataType' => $data_type, 'content' => array());
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

  if (count($constraints) == 0)
    $constraints['group'] = array('AnalysisOps');

  $html = file_get_contents(__DIR__ . '/html/inventory.html');

  $html = str_replace('${DATA_TYPE}', $data_type, $html);
  $html = str_replace('${CATEGORIES}', $categories, $html);
  $html = str_replace('${CONSTRAINTS}', json_encode($constraints), $html);

  echo $html;
}

?>