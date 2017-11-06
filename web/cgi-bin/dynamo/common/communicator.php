<?php

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
    $qstring ="SELECT u.`email` FROM users AS u INNER JOIN authorized_users as au WHERE lower(u.`name`) = lower('$user') AND u.`id` = au.`user_id`";
  }
  if (!execQuery($qstring,$db)){
   echo "Not a valid user."; echo "\n";
    exit();
  }
  else{
    return execQuery($qstring,$db);
  }
}

function communicate($filename,$db,$info,$type){
  $status = 'new';
  $qstring = 'insert into action(file,status,info,type) values'.
    '(\''.$filename.'\',\''.$status.'\',\''.$info.'\',\''.$type.'\')';
  echo "File successfully uploaded."; echo "\n";

  return execQuery($qstring,$db);
}

?>
