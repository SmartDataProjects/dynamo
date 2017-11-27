<?php

include('/var/www/cgi-bin/dynamo/common/db_conf.php');

$uploadpath = '/local/dynamo/interface/';
$db_name = 'dynamoregister';
$db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], $db_name);

function filecopy($s1,$s2) {
  $path = pathinfo($s2);
  if (!file_exists($path['dirname'])) {
    mkdir($path['dirname'], 0777, true);
  }
  if (!copy($s1,$s2)) {
    echo "copy failed \n";
  }
  else{
    chmod($path['dirname'], 0777);
    chmod($s2, 0777);
  }
}

function execQuery($qstring,$db){
  $locvar = 0;
  $stmt = $db->prepare($qstring);
  $stmt->bind_result($locvar);
  $stmt->execute();
  $stmt->store_result();

  if($stmt->num_rows < 2){
    $retvar = 0;
    while($stmt->fetch()){
      $retvar = $locvar;
    }
  }
  else if($stmt->num_rows > 1){
    $retvar = array();
    while($stmt->fetch()){
      $retvar[] = $locvar;
    }
  }
  $stmt->close();

  return $retvar;
}

function check_authentication($user,$db){
  if (!$user){
    echo "Something went wrong."; echo "\n";
    exit();
  }
  else{
    $qstring ="SELECT au.`user_id` FROM users AS u INNER JOIN authorized_users as au WHERE lower(u.`name`) = lower('$user') AND u.`id` = au.`user_id`";
  }
  if (!execQuery($qstring,$db)){
   echo "Not a valid user."; echo "\n";
    exit();
  }
  else{
    return execQuery($qstring,$db);
  }
}

function communicate($write,$title,$filename,$db,$username,$email,$args){
  $status = 'new';
  date_default_timezone_set("EST");
  $timestamp = date("Y-m-d H:i:s", time());

  $qstring = 'insert into action(write_request,title,path,status,user_id,timestamp,email,args) values'.
    '(\''.$write.'\',\''.$title.'\',\''.$filename.'\',\''.$status.'\',\''.$username.'\',\''.$timestamp.'\',\''.$email.'\',\''.$args.'\')';
  echo "File successfully uploaded."; echo "\n";

  return execQuery($qstring,$db);
}

?>
