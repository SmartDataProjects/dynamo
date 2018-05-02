<?php

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

?>