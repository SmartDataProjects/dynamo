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

function check_authentication($email,$db){
  if (!$email){
    echo "Please specify your email address for the results to be sent to."; echo "\n";
    exit();
  }
  else{
    $qstring ="SELECT 1 FROM users WHERE lower(`email`) = lower('$email')";
  }
  if (!execQuery($qstring,$db)){
   echo "Not a valid user."; echo "\n";
    exit();
  } 
}

function communicate($filename,$db,$info,$type){
  $status = 'new';
  $qstring = 'insert into action(file,status,info,type) values'.
    '(\''.$filename.'\',\''.$status.'\',\''.$info.'\',\''.$type.'\')';

  echo "File successfully uploaded. Results will be sent to $info. Be patient."; echo "\n";

  return execQuery($qstring,$db);

}

?>
