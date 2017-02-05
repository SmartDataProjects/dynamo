<?php

function send_header($code, $message)
{
  header($_SERVER['SERVER_PROTOCOL'] . ' ' . $code . ' ' . $message, true, $code);
}

if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS') {
  send_header(401, 'Failed authentication');
  exit(0);
}

include_once(__DIR__ . '/../common/db_conf.php');

if (isset($_REQUEST['action'])) {

  if (!isset($_REQUEST['item']) && !isset($_REQUEST['lockid'])) {
    send_header(400, 'Bad request - missing parameter (item or lockid)');
    echo "missing parameter (item or lockid)\n";
    exit(0);
  }

  if ($_REQUEST['action'] == 'release')
    $enabled = 0;
  else if ($_REQUEST['action'] == 'lock')
    $enabled = 1;
  else {
    send_header(400, 'Bad request - invalid action');
    exit(0);
  }

  $registry_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

  $dn = $_SERVER['SSL_CLIENT_S_DN'];
  $uid = 0;

  $stmt = $registry_db->prepare('SELECT `id` FROM `users` WHERE `dn` LIKE ?');
  $stmt->bind_param('s', $dn);
  $stmt->bind_result($uid);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($uid == 0) {
    send_header(400, 'Bad request - unknown user');
    exit(0);
  }

  if (isset($_REQUEST['lockid'])) {
    $id = 0 + $_REQUEST['lockid'];
  
    $stmt = $registry_db->prepare('SELECT COUNT(*) FROM `detox_locks` WHERE `id` = ?');
    $stmt->bind_param('i', $id);
    $stmt->bind_result($count);
    $stmt->execute();
    $stmt->fetch();
    $stmt->close();
    if ($count == 0) {
      send_header(400, 'Bad request - lock does not exist');
      exit(0);
    }
  }
  else if (isset($_REQUEST['item'])) {
    $item = $_REQUEST['item'];
    $sites = '';
    $groups = '';

    $query = 'SELECT `id` FROM `detox_locks` WHERE `item` LIKE ?';
    $params = array('s', $item);
    if (isset($_REQUEST['sites'])) {
      $sites = $_REQUEST['sites'];

      $query .= ' AND `sites` LIKE ?';
      $params[0] .= 's';
      $params[] = $sites;
    }
    if (isset($_REQUEST['groups'])) {
      $groups = $_REQUEST['groups'];

      $query .= ' AND `groups` LIKE ?';
      $params[0] .= 's';
      $params[] = $groups;
    }

    $stmt = $registry_db->prepare($query);
    call_user_func_array(array($stmt, "bind_param"), $params);
    $stmt->bind_result($id);
    $stmt->execute();
    $match = $stmt->fetch();
    $stmt->close();
    if (!$match) {
      // this is a new lock

      if (isset($_REQUEST['expire'])) {
        $date = strtotime($_REQUEST['expire']);
        if ($date === false) {
          send_header(400, 'Bad request - expiration date ill-formatted');
          exit(0);
        }

        $expiration = strftime('%Y-%m-%d %H:%M:%S', $date);
      }
      else {
        send_header(400, 'Bad request - new locks must be created with expire= option');
        exit(0);
      }

      $comment = '';
      if (isset($_REQUEST['comment']))
        $comment = $_REQUEST['comment'];

      $query = 'INSERT INTO `detox_locks` (`enabled`, `item`, `sites`, `groups`, `entry_date`, `expiration_date`, `user_id`, `comment`) VALUES (?, ?, ?, ?, NOW(), ?, ?, ?)';
      $stmt = $registry_db->prepare($query);
      $stmt->bind_param('issssis', $enabled, $item, $sites, $groups, $expiration, $uid, $comment);
      $stmt->execute();
      $stmt->close();

      send_header(200, 'OK');
      echo "Lock updated";

      exit(0);
    }
  }

  // known lock - update
  $stmt = $registry_db->prepare('UPDATE `detox_locks` SET `enabled` = ? WHERE `id` = ?');
  $stmt->bind_param('ii', $enabled, $id);
  $stmt->execute();
  $stmt->close();

  send_header(200, 'OK');
  echo "Lock updated";
}
else if (isset($_REQUEST['data'])) {
  $registry_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

  $enabled = array();

  $query = 'SELECT u.`name`, l.`item`, l.`sites`, l.`groups`, l.`entry_date`, l.`expiration_date`, l.`comment` FROM `detox_locks` AS l';
  $query .= ' INNER JOIN `users` AS u ON u.`id` = l.`user_id`';
  $query .= ' WHERE l.`enabled` = 1 ORDER BY l.`entry_date` DESC';
  $stmt = $registry_db->prepare($query);
  $stmt->bind_result($uname, $item, $sites, $groups, $entry, $expiration, $comment);
  $stmt->execute();
  while ($stmt->fetch())
    $enabled[] = array('user' => $uname, 'item' => $item, 'sites' => $sites, 'groups' => $groups, 'entryDate' => $entry, 'expirationDate' => $expiration, 'comment' => $comment);
  $stmt->close();

  $disabled = array();

  $query = 'SELECT u.`name`, l.`item`, l.`sites`, l.`groups`, l.`entry_date`, l.`expiration_date`, l.`comment` FROM `detox_locks` AS l';
  $query .= ' INNER JOIN `users` AS u ON u.`id` = l.`user_id`';
  $query .= ' WHERE l.`enabled` = 0 ORDER BY l.`entry_date` DESC';
  $stmt = $registry_db->prepare($query);
  $stmt->bind_result($uname, $item, $sites, $groups, $entry, $expiration, $comment);
  $stmt->execute();
  while ($stmt->fetch())
    $disabled[] = array('user' => $uname, 'item' => $item, 'sites' => $sites, 'groups' => $groups, 'entryDate' => $entry, 'expirationDate' => $expiration, 'comment' => $comment);
  $stmt->close();

  echo json_encode(array('enabled' => $enabled, 'disabled' => $disabled));
}
else {
  $html = file_get_contents(__DIR__ . '/html/detoxlock.html');

  echo $html;
}
  
?>