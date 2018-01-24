<?php

include_once(__DIR__ . '/../common/db_conf.php');

$db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

$join_clause = array();
$where_clause = array();

if (isset($_REQUEST['user_name'])) {
  $user_name = $_REQUEST['user_name'];
  $join_clause['users'] = 'INNER JOIN `users` ON `users`.`id` = `detox_locks`.`user_id`';
  $where_clause[] = sprintf('`users`.`name` = "%s"', $user_name);
}

$locked = "y";
if (isset($_REQUEST['locked']))
  $locked = $_REQUEST['locked'];

if ($locked == "y")
  $where_clause[] = '`unlock_date` IS NULL';
else if ($locked == "n")
  $where_clause[] = '`unlock_date` IS NOT NULL';
else {
  // invalid value
}

if (isset($_REQUEST['locked_since']))
  $where_clause[] = sprintf('`lock_date` >= "%s"', $_REQUEST['locked_since']);

if (isset($_REQUEST['locked_before']))
  $where_clause[] = sprintf('`lock_date` <= "%s"', $_REQUEST['locked_before']);

if (isset($_REQUEST['unlocked_since']))
  $where_clause[] = sprintf('`unlock_date` >=  "%s"', $_REQUEST['unlocked_since']);

if (isset($_REQUEST['unlocked_before']))
  $where_clause[] = sprintf('`unlock_date` <=  "%s"', $_REQUEST['unlocked_before']);

if (isset($_REQUEST['site']))
  $where_clause[] = sprintf('`sites` = "%s"', $_REQUEST['site']);

if (isset($_REQUEST['expires_after']))
  $where_clause[] = sprintf('`expiration_date` > "%s"', $_REQUEST['expires_after']);

if (isset($_REQUEST['expires_before']))
  $where_clause[] = sprintf('`expiration_date` <  "%s"', $_REQUEST['expires_before']);

if (isset($_REQUEST['item']))
  $where_clause[] = sprintf('`item` LIKE "%s" ', str_replace("*", "%", $_REQUEST['item']));

$final_join_clause = implode(' ', $join_clause);
$final_where_clause = ' WHERE ' . implode(" AND ", $where_clause);

$query = 'SELECT `item`, `sites`, `lock_date`, `unlock_date`, `expiration_date`, `comment`, `name`, `email`';
$query .= ' FROM `detox_locks` ' . $final_join_clause;
$query .= $final_where_clause;

$html = file_get_contents(__DIR__ . '/html/locks.html');

$table_cont = '';

$stmt = $db->prepare($query);
$stmt->bind_result($item, $sites, $lockdate, $unlockdate, $expiration_date, $comment, $name, $email);
$stmt->execute();
while($stmt->fetch()) {
  if ($unlockdate === NULL)
    $unlockdate_td = '<td style="text-align:center;">---</td>';
  else
    $unlockdate_td = '<td>' . $unlockdate . '</td>';

  $table_cont .= '<tr><td>' . $name . '</td><td>' . $email . '</td><td>' . $item . '</td><td>' . $sites . '</td><td>' . $lockdate . '</td>' . $unlockdate_td . '<td>' . $expiration_date . '</td><td>' . $comment . '</td> </tr>';
}
$stmt->close();

$html = str_replace('${TABLE_CONT}', "$cycle", $table_cont);

echo $html;

?>
