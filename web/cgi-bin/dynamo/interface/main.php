<?php

include('/var/www/cgi-bin/dynamo/common/db_conf.php');
include('/var/www/cgi-bin/dynamo/common/communicator.php');

$db_name = 'dynamoregister';
$db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $db_name);

$uploadpath = "/var/www/html/dynamo/dynamo/images/";
//echo $_REQUEST['filename'];
$filedata = $_FILES['file']['tmp_name'];
$filename = $_FILES['file']['name'];
if ($filedata != ''){
  $hash = hash_file('md5',$filedata);
  copy($filedata,$uploadpath.$hash);
  communicate($hash,$db);
}
else{
  $html = file_get_contents(__DIR__ . '/html/interface.html');
  echo $html;
}

?>
