<?php

date_default_timezone_set('UTC');

$command = substr($_SERVER['PATH_INFO'], 1); # dynamo.mit.edu/registry/detoxlock/command -> /command

if ($command == "") {
  # show webpage
  exit(0);
}

if ($command == 'help') {
  echo file_get_contents(__DIR__ . '/html/detoxlock_help.html');
  exit(0);
}

include_once(__DIR__ . '/common.php');

if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS')
  send_response(401, 'AuthFailed', 'SSL authentication failed.');

include_once('detoxlock.class.php');

if (isset($_REQUEST['service'])) {
  $service = $_REQUEST['service'];
  // $request['service'] is used to look up locks when command = list
}
else
  $service = 'user';

// admin users can specify to act as another user
if (isset($_REQUEST['asuser'])) {
  $as_user = $_REQUEST['asuser'];
  unset($_REQUEST['asuser']);
}
else
  $as_user = NULL;

// CLIENT_S_DN: DN of the client cert (can be a proxy)
// CLIENT_I_DN: DN of the issuer of the client cert
$detoxlock = new DetoxLock($_SERVER['SSL_CLIENT_S_DN'], $_SERVER['SSL_CLIENT_I_DN'], $service, $as_user);

if (isset($_REQUEST['format'])) {
  if (in_array($_REQUEST['format'], array('json', 'xml')))
    $detoxlock->format = $_REQUEST['format'];
  else
    $detoxlock->send_response(400, 'BadRequest', 'Unknown format');

  unset($_REQUEST['format']);
}

if (isset($_REQUEST['return'])) {
  $request = $_REQUEST['return'];
  if ($request != 'yes' && $request != 'no')
    $detoxlock->send_response(400, 'BadRequest', 'Unknown value for option return');

  $detoxlock->return_data = ($request == 'yes');

  unset($_REQUEST['return']);
}

$detoxlock->execute($command, $_REQUEST);
  
?>
