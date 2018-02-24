<?php

date_default_timezone_set('UTC');

$command = substr($_SERVER['PATH_INFO'], 1); # dynamo.mit.edu/registry/invalidate/command -> /command

if ($command == "") {
  # show webpage
  exit(0);
}

if ($command == 'help') {
#  echo file_get_contents(__DIR__ . '/html/invalidate_help.html');
  exit(0);
}

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

/* if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS') */
/*   send_response(401, 'AuthFailed', 'SSL authentication failed.'); */

if ($command == 'invalidate' || $command == 'clear') {
  if (!isset($_REQUEST['item']))
    send_response(400, 'BadRequest', '\'item\' field is required', NULL, 'json');

  $item = $_REQUEST['item'];

  $db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

  if ($command == 'invalidate') {
    $stmt = $db->prepare('INSERT INTO `invalidations` (`item`, `timestamp`) VALUES (?, NOW())');
    $message = 'Item added';   
  }
  else {
    $stmt = $db->prepare('DELETE FROM `invalidations` WHERE `item` = ?');
    $message = 'Item removed';
  }

  $return_data = array();

  if (is_array($item)) {
    foreach ($item as $t) {
      $stmt->bind_param('s', $t);
      $stmt->execute();
      $return_data[] = array('item' => $t);
    }
  }
  else {
    $stmt->bind_param('s', $item);
    $stmt->execute();
    $return_data[] = array('item' => $item);
  }
  $stmt->close();

  send_response(200, 'OK', $message, $return_data, 'json');
}
else {
  send_response(400, 'BadRequest', 'Invalid command (possible values: help, invalidate)');
}

?>