<?php

include('/var/www/cgi-bin/dynamo/common/communicator.php');

$username=$_SERVER['SSL_CLIENT_S_DN_CN'];
$title = $_REQUEST['title'];
$write = min(1,intval($_REQUEST['write']));
$email = $_REQUEST['email'];
$args = $_REQUEST['args'];

if(!$title){
  $title = 'DynamoInteraction';//generic interaction
}

$filedata = $_FILES['file']['tmp_name'];
$filename = $_FILES['file']['name'];

if ($filedata != ''){
  $userid = check_authentication($username,$db);
  $hash = hash_file('md5',$filedata);
  $rand = rand(1,10000000);

  while (is_dir($uploadpath.$hash.$rand)){
    $rand = rand(1,10000000);
  }

  if (!filecopy($filedata,$uploadpath.$hash.$rand."/exec.py")){
    communicate($write,$title,$hash.$rand,$db,$userid,$email,$args);
  }
  else{
    echo "Something went wrong."; echo "\n";
  }
  //  copy($filedata,$uploadpath.$hash);
}
else{
  $html = file_get_contents(__DIR__ . '/html/interface.html');
  $html = str_replace('${USER}',"$username",$html);
  echo $html;
}

?>
