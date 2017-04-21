<?php

$db_conf_name = 'mysql-dynamo';

$mycnf = file('/etc/my.cnf');
$db_conf = array();
$in_block = false;
foreach ($mycnf as $line) {
  if (trim($line) == '')
    continue;

  if (strpos($line, '[mysql-dynamo]') !== false) {
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

?>
