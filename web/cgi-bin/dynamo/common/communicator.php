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

function communicate($filename,$db){
  $status = 'new';
  $qstring = 'insert into action(file,status) values'.
    '(\''.$filename.'\',\''.$status.'\')';
  echo $qstring; echo "\n";
  return execQuery($qstring,$db);
}

?>
