<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

$username = $_SERVER['SSL_CLIENT_S_DN_CN'];

$filedata = $_FILES['file']['tmp_name'];
$filename = $_FILES['file']['name'];

if ($filedata != '') {
  $db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

  $uid = 0;
  $uname = '';
  $sid = 0;

  get_user($db, $_SERVER['SSL_CLIENT_S_DN'], $_SERVER['SSL_CLIENT_I_DN'], "user", NULL, $uid, $uname, $sid);

  $hash = hash_file('md5', $filedata);
  $rand = rand(1, 10000000);

  $uploadpath = '/var/spool/dynamo/';

  $execdir = $uploadpath . $hash . $rand;

  while (is_dir($execdir)) {
    $rand = rand(1, 10000000);
    $execdir = $uploadpath . $hash . $rand;
  }

  $execpath = $execdir . "/exec.py";

  $path = pathinfo($execpath);
  if (!file_exists($path['dirname'])) {
    mkdir($path['dirname'], 0777, true);
  }

  if (!copy($filedata, $execpath)) {
    echo "copy failed \n";
  }
  else {
    chmod($path['dirname'], 0777);
    chmod($execpath, 0777);
  }

  $title = $_REQUEST['title'];
  $write = min(1, intval($_REQUEST['write']));
  $email = $_REQUEST['email'];
  $args = $_REQUEST['args'];

  if (!$title) {
    $title = 'DynamoInteraction'; // generic interaction
  }

  $query = 'INSERT INTO `action` (`title`, `path`, `args`, `write_request`, `user_id`, `email`)';
  $query .= ' VALUES (?, ?, ?, ?, ?, ?)';

  $stmt = $db->prepare($query);
  $stmt->bind_param('sssiis', $title, $execdir, $args, $write, $uid, $email);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  echo "File uploaded to " . $execpath . ".\n";
}
else {
  $html = file_get_contents(__DIR__ . '/html/interface.html');
  $html = str_replace('${USER}', "$username", $html);
  echo $html;
}

?>
