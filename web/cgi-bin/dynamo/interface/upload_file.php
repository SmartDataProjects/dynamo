<?php

include('/var/www/cgi-bin/dynamo/common/db_conf.php');
include('/var/www/cgi-bin/dynamo/common/communicator.php');

$uploadpath = '/var/www/html/dynamo/dynamo/images/';
$filename=$_FILES['file']['name'];
$filedata = $_FILES['file']['tmp_name'];

if ($_FILES['file']['error'] > 0)
  {
    echo "Error: " . $_FILES['file']['error'] . "<br>";
  }
elseif ( end(explode('.', $filename)) != "txt" )
  {
    echo "<div style ='font:20px/21px Arial,tahoma,sans-serif;color:#000000'> Error: file $filename does not have the correct type.</div>" . "<br>";
    echo "<div style ='font:20px/21px Arial,tahoma,sans-serif;color:#000000'> It has to be a .txt file.</div>" . "<br>";
  }
else
  {
    $db_name = 'dynamoregister';
    $db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $db_name);

    $email=$_POST['email'];
    check_authentication($email,$db);
    $hash = hash_file('md5',$filedata);
    if (copy($filedata,$uploadpath.$hash)){
      communicate($hash,$db,$email,"dc");
    }
    else{
      echo "Something went wrong.";
    }
  }
?>
