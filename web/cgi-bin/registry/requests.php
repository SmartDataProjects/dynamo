<?php

date_default_timezone_set('UTC');

$command = substr($_SERVER['PATH_INFO'], 1); # dynamo.mit.edu/registry/request/command -> /command

if ($command == "") {
  # show webpage
  exit(0);
}

if ($command == 'help') {
  echo file_get_contents(__DIR__ . '/html/requests_help.html');
  exit(0);
}

include_once(__DIR__ . '/common.php');

if ($_SERVER['SSL_CLIENT_VERIFY'] != 'SUCCESS')
  send_response(401, 'AuthFailed', 'SSL authentication failed.');

include_once('requests.class.php');

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
$requests = new Requests($_SERVER['SSL_CLIENT_S_DN'], $_SERVER['SSL_CLIENT_I_DN'], $service, $as_user);

$requests->execute($command, $_REQUEST);

?>
