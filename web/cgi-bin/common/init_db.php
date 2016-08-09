<?php

include_once('config.php');

$mycnf = file('/etc/my.cnf');
$db_conf = array();
$in_block = false;
foreach ($mycnf as $line) {
  if (trim($line) == '')
    continue;

  if (strpos($line, '[' . $db_conf_name . ']') !== false) {
    $in_block = true;
    continue;
  }
  else if (strpos($line, '[') === 0)
    $in_block = false;

  if ($in_block) {
    $words = explode('=', rtrim($line));
    $db_conf[$words[0]] = $words[1];
  }
}

$history_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $history_db_name);
$store_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $store_db_name);

?>
