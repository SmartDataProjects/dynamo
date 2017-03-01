<?php

function send_response($code, $result, $message, $data = NULL)
{
  header($_SERVER['SERVER_PROTOCOL'] . ' ' . $code, true, $code);
  $response = array('result' => $result, 'message' => $message);
  if ($data !== NULL)
    $response['data'] = $data;

  $json = '{"result": "' . $result . '", "message": "' . $message . '", "data": [';
  $data_json = array();
  foreach ($data as $elem) {
    $j = array();
    foreach($elem as $key => $value) {
      $kv = '"' . $key .'": ';
      if (is_string($value))
        $kv .= '"' . $value . '"';
      else
        $kv .= '' . $value;

      $j[] = $kv;
    }
    $data_json[] = '{' . implode(', ', $j) . '}';
  }
  $json .= implode(', ', $data_json);
  $json .= ']}';
  
  echo $json . "\n";
  exit(0);
}

if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS')
  send_response(401, 'AuthFailed', 'SSL authentication failed');

$command = substr($_SERVER['PATH_INFO'], 1); # dynamo.mit.edu/registry/detoxlock/command -> /command

if ($command == "") {
  # show webpage
  exit(0);
}

if (!in_array($command, array('protect', 'release', 'list', 'set')))
  send_response(400, 'BadRequest', 'Invalid command (possible values: protect, release, list, set)');

include_once(__DIR__ . '/../dynamo/common/db_conf.php');

$registry_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

$sdn = $_SERVER['SSL_CLIENT_S_DN']; // DN of the client cert (can be a proxy)
$idn = $_SERVER['SSL_CLIENT_I_DN']; // DN of the issuer of the client cert
$uid = 0;

$stmt = $registry_db->prepare('SELECT `id`, `name` FROM `users` WHERE `dn` LIKE ? OR `dn` LIKE ?');
$stmt->bind_param('ss', $sdn, $idn);
$stmt->bind_result($uid, $uname);
$stmt->execute();
$stmt->fetch();
$stmt->close();

if ($uid == 0)
  send_response(400, 'BadRequest', 'Unknown user');

function lock_lock($val)
{
  global $registry_db;

  $stmt = $registry_db->prepare('UPDATE `detox_locks_lock` SET `updating` = ?');
  $stmt->bind_param('i', $val);
  $stmt->execute();
  $stmt->close();
}

function get_lock_data($lockid, $item = '', $sites = '', $groups = '', $created_before = 0, $created_after = 0, $expires_before = 0, $expires_after = 0)
{
  global $registry_db;
  global $uid;

  $data = array();

  $query = 'SELECT l.`id`, l.`enabled`, l.`item`, l.`sites`, l.`groups`, l.`entry_date`, l.`expiration_date`, u.`name`, l.`comment` FROM `detox_locks` AS l INNER JOIN `users` AS u ON u.`id` = l.`user_id` WHERE ';

  if ($lockid != 0) {
    $query .= 'l.`id` = ?';
    $stmt = $registry_db->prepare($query);
    $stmt->bind_param('i', $lockid);
  }
  else {
    $query .= 'u.`id` = ?';

    $params = array('i', &$uid);

    if ($item != '') {
      $query .= ' AND l.`item` LIKE ?';
      $params[0] .= 's';
      $params[] = &$item;
    }

    if ($sites != '') {
      $query .= ' AND l.`sites` LIKE ?';
      $params[0] .= 's';
      $params[] = &$sites;
    }

    if ($groups != '') {
      $query .= ' AND l.`groups` LIKE ?';
      $params[0] .= 's';
      $params[] = &$groups;
    }

    if ($created_before != 0) {
      $query .= ' AND l.`entry_date` <= FROM_UNIXTIME(?)';
      $params[0] .= 'i';
      $params[] = &$created_before;
    }

    if ($created_after != 0) {
      $query .= ' AND l.`entry_date` >= FROM_UNIXTIME(?)';
      $params[0] .= 'i';
      $params[] = &$created_after;
    }

    if ($expires_before != 0) {
      $query .= ' AND l.`expiration_date` <= FROM_UNIXTIME(?)';
      $params[0] .= 'i';
      $params[] = &$expires_before;
    }

    if ($expires_after != 0) {
      $query .= ' AND l.`expiration_date` >= FROM_UNIXTIME(?)';
      $params[0] .= 'i';
      $params[] = &$expires_after;
    }

    $stmt = $registry_db->prepare($query);
    call_user_func_array(array($stmt, "bind_param"), $params);
  }

  $stmt->bind_result($lockid, $enabled, $item, $sites, $groups, $entry, $expiration, $uname, $comment);
  $stmt->execute();
  while ($stmt->fetch())
    $data[$lockid] = array('lockid' => $lockid, 'enabled' => $enabled, 'user' => $uname, 'item' => $item, 'sites' => $sites, 'groups' => $groups, 'created' => $entry, 'expires' => $expiration, 'comment' => $comment);
  $stmt->close();

  return $data;
}

function create_lock($item, $sites, $groups, $expiration, $comment)
{
  global $registry_db;
  global $uid;

  $query = 'INSERT INTO `detox_locks` (`enabled`, `item`, `sites`, `groups`, `entry_date`, `expiration_date`, `user_id`, `comment`) VALUES (1, ?, ?, ?, NOW(), ?, ?, ?)';
  $stmt = $registry_db->prepare($query);
  $stmt->bind_param('ssssis', $item, $sites, $groups, $expiration, $uid, $comment);
  $stmt->execute();
  $lockid = $stmt->insert_id;
  $stmt->close();

  return $lockid;
}

function update_lock($lockid, $enabled = NULL, $expiration = NULL, $comment = NULL)
{
  $query = 'UPDATE `detox_locks` SET ';
  $params = array('');

  $set = array();

  if ($enabled !== NULL) {
    $set[] = '`enabled` = ?';
    $params[0] .= 'i';
    $params[] = &$enabled;
  }

  if ($expiration !== NULL) {
    $set[] = '`expiration_date` = ?';
    $params[0] .= 'i';
    $params[] = &$expiration;
  }

  if ($comment !== NULL) {
    $set[] = '`comment` = ?';
    $params[0] .= 's';
    $params[] = &$comment;
  }

  $query .= implode(', ', $set);

  $query .= ' WHERE `id` = ?';
  $params[0] .= 'i';
  $params[] = &$lockid;

  $stmt = $registry_db->prepare('`enabled` = ? WHERE `id` = ?');
  call_user_func_array(array($stmt, "bind_param"), $params);
  $stmt->execute();
  $stmt->close();

  $data = get_lock_data($lockid);

  return (($enabled === NULL || $data['enabled'] == $enabled) &&
          ($expiration === NULL || $data['expires'] == $expiration) &&
          ($comment === NULL || $data['comment'] == $comment));
}


if ($command == 'protect' || $command == 'release') {
  $lockid = 0;

  if (isset($_REQUEST['lockid'])) {
    $lockid = 0 + $_REQUEST['lockid'];
    $data = get_lock_data($lockid);

    if (count($data) == 0)
      $lockid = 0;
  }
  else if (isset($_REQUEST['item'])) {
    $item = $_REQUEST['item'];
    $sites = isset($_REQUEST['sites']) ? $_REQUEST['sites'] : '';
    $groups = isset($_REQUEST['groups']) ? $_REQUEST['groups'] : '';
    
    $data = get_lock_data(0, $item, $sites, $groups);
    $lock = current($data);
    $lockid = $lock['lockid'];
  }
  else
    send_response(400, 'BadRequest', 'Missing parameter (item or lockid)');

  if ($lockid != 0) {
    if ($command == 'protect')
      $enabled = 1;
    else
      $enabled = 0;

    // known lock - update
    if (update_lock($lockid, $enabled))
      send_response(200, 'OK', 'Lock updated', array_values(get_lock_data($lockid)));
    else
      send_response(400, 'InternalError', 'Failed to update lock');
  }
  else if ($command == 'release') {
    send_response(400, 'BadRequest', 'Lock does not exist');
  }
  else {
    // this is a new lock

    if (isset($_REQUEST['expires'])) {
      if (is_numeric($_REQUEST['expires']))
        $date = 0 + $_REQUEST['expires'];
      else {
        $date = strtotime($_REQUEST['expires']);
        if ($date === false)
          send_response(400, 'BadRequest', 'Expiration date ill-formatted');
      }

      if ($date < time())
        send_response(400, 'BadRequest', 'Expiration date must be in the future');

      $expiration = strftime('%Y-%m-%d %H:%M:%S', $date);
    }
    else
      send_response(400, 'BadRequest', 'Expiration date not set');

    $comment = '';
    if (isset($_REQUEST['comment']))
      $comment = $_REQUEST['comment'];

    $lockid = create_lock($item, $sites, $groups, $expiration, $comment);

    if ($lockid != 0)
      send_response(200, 'OK', 'Lock created', array_values(get_lock_data($lockid)));
    else
      send_response(400, 'InternalError', 'Failed to create lock');
  }
}
else if ($command == 'list') {
  $item = isset($_REQUEST['item']) ? $_REQUEST['item'] : '';
  $sites = isset($_REQUEST['sites']) ? $_REQUEST['sites'] : '';
  $groups = isset($_REQUEST['groups']) ? $_REQUEST['groups'] : '';

  $timestamps = array('created_before' => 0, 'created_after' => 0, 'expires_before' => 0, 'expires_after' => 0);
  foreach (array_keys($timestamps) as $key) {
    if (!isset($_REQUEST[$key]))
      continue;

    if (is_numeric($_REQUEST[$key]))
      $timestamps[$key] = 0 + $_REQUEST[$key];
    else {
      $timestamp = strtotime($_REQUEST[$key]);
      if ($timestamp === false)
        send_response(400, 'BadRequest', 'Ill-formatted date string for ' . $key);

      $timestamps[$key] = $timestamp;
    }
  }

  $data = get_lock_data(0, $item, $sites, $groups, $timestamps['created_before'], $timestamps['created_after'], $timestamps['expires_before'], $timestamps['expires_after']);
  if (count($data) == 0)
    send_response(200, 'EmptyResult', 'No lock found');
  else
    send_response(200, 'OK', count($data) . ' locks found', array_values($data));
}
else if ($command == 'set') {
  if (!isset($_POST['data']) || !is_array($_POST['data']))
    send_response(400, 'BadRequest', 'No data posted');

  lock_lock(1);

  $inserting_entries = array();
  $updating_entries = array();

  $data = $_POST['data'];
  foreach ($data as $key => $entry) {
    if (!isset($entry['item'])) {
      lock_lock(0);
      send_response(400, 'BadRequest', 'Missing item name in entry ' . $key);
    }

    $sites = isset($entry['sites']) ? $entry['sites'] : '';
    $groups = isset($entry['groups']) ? $entry['groups'] : '';

    $current = get_lock_data(0, $entry['item'], $sites, $groups);

    if (count($current) == 0) {
      // new lock
      if (!isset($entry['expires'])) {
        lock_lock(0);
        send_response(400, 'BadRequest', 'Missing expiry date in entry ' . $key);
      }
  
      if (is_numeric($entry['expires']))
        $expires = 0 + $entry['expires'];
      else {
        $expires = strtotime($entry['expires']);
        if ($expires === false) {
          lock_lock(0);
          send_response(400, 'BadRequest', 'Ill-formatted expiry date string');
        }
      }

      $comment = isset($entry['comment']) ? $entry['comment'] : '';

      $inserting_entries[] = array($entry['item'], $sites, $groups, $expires, $comment);
    }
    else {
      $update = current($current);

      if (isset($entry['enabled'])) {
        if ($entry['enabled'])
          $update['enabled'] = 1;
        else
          $update['enabled'] = 0;
      }

      if (isset($entry['expires'])) {
        if (is_numeric($entry['expires']))
          $expires = 0 + $entry['expires'];
        else {
          $expires = strtotime($entry['expires']);
          if ($expires === false) {
            lock_lock(0);
            send_response(400, 'BadRequest', 'Ill-formatted expiry date string');
          }
        }

        $update['expires'] = $expires;
      }
      else if (!isset($update['expires']))
        $update['expires'] = NULL;

      if (isset($entry['comment']))
        $update['comment'] = $entry['comment'];
      else if (!isset($update['comment']))
        $update['comment'] = NULL;

      $updating_entries[$update['lockid']] = $update;
    }
  }

  $existing_entries = get_lock_data(0);

  $deleting_entries = array();

  foreach (array_keys($existing_entries) as $key) {
    if (!array_key_exists($key, $updating_entries))
      $deleting_entries[] = $key;
  }

  foreach ($inserting_entries as $entry) {
    $lockid = create_lock($entry[0], $entry[1], $entry[2], $entry[3], $entry[4]);
    if ($lockid == 0) {
      lock_lock(0);
      send_response(400, 'InternalError', 'Failed to create lock');
    }
  }

  foreach ($updating_entries as $lockid => $entry) {
    if (!update_lock($lockid, $entry['enabled'], $entry['expires'], $entry['comment']))
      send_response(400, 'InternalError', 'Failed to update lock');
  }

  foreach ($deleting_entries as $lockid) {
    if (!update_lock($lockid, 0))
      send_response(400, 'InternalError', 'Failed to disable lock');
  }
}
  
?>