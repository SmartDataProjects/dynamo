<?php

include('/var/www/cgi-bin/dynamo/common/db_conf.php');
include('/var/www/cgi-bin/dynamo/common/communicator.php');

if ($_FILES["file"]["error"] > 0)
  {
    echo "Error: " . $_FILES["file"]["error"] . "<br>";
  }
else
  {
    echo $_REQUEST['emailAddress'];
    $uploadpath = '/var/www/html/dynamo/dynamo/images/';
    //$uploadpath = '/var/spool/dynamo/interfaced_actions/';
    $filedata = $_FILES['file']['tmp_name'];
    $filename = $_FILES['file']['name'];
    if (copy($filedata,$uploadpath.$filename)){
      $hash = hash_file('md5',$uploadpath.$filename);
      communicate($hash,$db);
      echo "Successfully uploaded. Awaiting results.";
    }
    else{
      echo "Something went wrong.";
    }
  }
?>
