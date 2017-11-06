<?php

include('/var/www/cgi-bin/dynamo/common/db_conf.php');
include('/var/www/cgi-bin/dynamo/common/communicator.php');

$uploadpath = "/var/www/html/dynamo/dynamo/images/";
$db_name = 'dynamoregister';
$db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $db_name);

$username=$_SERVER['SSL_CLIENT_S_DN_CN'];
$type = $_REQUEST['service'];

if(!$type){
  $type = 'gi';//generic interaction
}

$filedata = $_FILES['file']['tmp_name'];
$filename = $_FILES['file']['name'];
if ($filedata != ''){
  check_authentication($username,$db);
  $hash = hash_file('md5',$filedata);
  if (file_exists($uploadpath.$hash)){
    echo "File already exists. Probably it is just being acted upon."; echo "\n";
    exit;
  }
  copy($filedata,$uploadpath.$hash);
  communicate($hash,$db,$username,$type);
}
else{
  //print_r($_SERVER);
  //echo $_SERVER['SSL_CLIENT_S_DN_CN'];
  $html = file_get_contents(__DIR__ . '/html/interface.html');
  $html = str_replace('${USER}',"$username",$html);
  echo $html;
}

?>
