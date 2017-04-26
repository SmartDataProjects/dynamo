<?php

date_default_timezone_set('UTC');

$command = substr($_SERVER['PATH_INFO'], 1); # dynamo.mit.edu/registry/detoxlock/command -> /command

if ($command == "") {
  # show webpage
  exit(0);
}

if ($command == 'help') {
  echo file_get_contents(__DIR__ . '/html/activitylock_help.html');
  exit(0);
}

include_once(__DIR__ . '/common.php');

if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS')
  send_response(401, 'AuthFailed', 'SSL authentication failed.');

include_once('activitylock.class.php');

if (isset($_REQUEST['service']))
  $service = $_REQUEST['service'];
else
  $service = 'user';
  
// admin users can specify to act as another user
if (isset($_REQUEST['asuser']))
  $as_user = $_REQUEST['asuser'];
else
  $as_user = NULL;

$activitylock = new ActivityLock($_SERVER['SSL_CLIENT_S_DN'], $_SERVER['SSL_CLIENT_I_DN'], $service, $as_user);

$activitylock->execute($command, $_REQUEST);

?>
