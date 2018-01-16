<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

$username = $_SERVER['SSL_CLIENT_S_DN_CN'];

$filedata = '';
if (isset($_FILES) && isset($_FILES['file'])) {
  $filedata = $_FILES['file']['tmp_name'];
  $filename = $_FILES['file']['name'];
}

if ($filedata == '' && !isset($_REQUEST['taskid'])) {
  $html = file_get_contents(__DIR__ . '/html/interface.html');
  $html = str_replace('${USER}', "$username", $html);
  echo $html;
  exit(0);
}

$db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

$uid = 0;
$uname = '';
$sid = 0;

$authorized = get_user($db, $_SERVER['SSL_CLIENT_S_DN'], $_SERVER['SSL_CLIENT_I_DN'], "user", NULL, $uid, $uname, $sid);

if ($uid == 0 || $sid == 0 || !$authorized)
  send_response(400, 'BadRequest', 'Unauthorized user', NULL, 'json');

$local = ($_SERVER['REMOTE_ADDR'] == $_SERVER['SERVER_ADDR']);

if ($filedata != '') {

  $execdir = '';

  if ($local && isset($_REQUEST['workdir'])) {
    // request from the local machine - allow path spec
    $execdir = $_REQUEST['workdir'];
  }

  if ($execdir == '') {
    $hash = hash_file('md5', $filedata);
    $rand = rand(1, 10000000);
  
    $uploadpath = '/var/spool/dynamo/';
  
    $execdir = $uploadpath . $hash . $rand;
  
    while (is_dir($execdir)) {
      $rand = rand(1, 10000000);
      $execdir = $uploadpath . $hash . $rand;
    }
  }

  $old_umask = umask(0);

  if (!file_exists($execdir))
    mkdir($execdir, 0777, true);

  $execpath = $execdir . "/exec.py";

  if (!copy($filedata, $execpath))
    send_response(500, 'ServerError', 'Upload failed', NULL, 'json');
  else {
    chmod($execdir, 0777);
    chmod($execpath, 0777);
  }

  umask($old_umask);

  $title = isset($_REQUEST['title']) ? $_REQUEST['title'] : '';
  $write_request = isset($_REQUEST['write']) ? min(1, intval($_REQUEST['write'])) : 0;
  $email = isset($_REQUEST['email']) ? $_REQUEST['email'] : '';
  $args = isset($_REQUEST['args']) ? $_REQUEST['args'] : '';

  if (!$title) {
    $title = 'DynamoInteraction'; // generic interaction
  }

  $query = 'INSERT INTO `action` (`title`, `path`, `args`, `write_request`, `user_id`, `email`)';
  $query .= ' VALUES (?, ?, ?, ?, ?, ?)';

  $stmt = $db->prepare($query);
  $stmt->bind_param('sssiis', $title, $execdir, $args, $write_request, $uid, $email);
  $stmt->execute();
  $task_id = $stmt->insert_id;
  $stmt->close();

  $data = array('taskid' => $task_id, 'title' => $title, 'args' => $args, 'write_request' => $write_request, 'email' => $email, 'status' => 'new');

  if ($local)
    $data['path'] = $execdir;

  send_response(200, 'OK', 'Task scheduled', array($data), 'json');

}
else if (isset($_REQUEST['taskid'])) {

  $task_id = 0 + $_REQUEST['taskid'];

  $db->query('LOCK TABLES `action` WRITE');

  $query = 'SELECT `title`, `args`, `write_request`, `email`, `status`, `exit_code`, `path` FROM `action`';
  $query .= ' WHERE `id` = ? AND `user_id` = ?';

  $stmt = $db->prepare($query);
  $stmt->bind_param('ii', $task_id, $uid);
  $stmt->bind_result($title, $args, $write_request, $email, $status, $exit_code, $path);
  $stmt->execute();
  $task_found = $stmt->fetch();
  $stmt->close();

  if ($email === NULL)
    $email = '';

  if ($task_found) {
    // prepare the return data
    $data = array('taskid' => $task_id, 'title' => $title, 'args' => $args, 'write_request' => $write_request, 'email' => $email, 'status' => $status, 'exit_code' => $exit_code);
    if ($local)
      $data['path'] = $path;

    if (isset($_REQUEST['action']) && $_REQUEST['action'] == 'kill') {
      if ($status == 'new' || $status == 'run') {
        $query = 'UPDATE `action` SET `status` = \'killed\' WHERE `id` = ?';
    
        $stmt = $db->prepare($query);
        $stmt->bind_param('i', $task_id);
        $stmt->execute();
        $stmt->close();

        $data[0]['status'] = 'killed';
        $message = 'Task aborted.';
      }
      else {
        if ($exit_code === NULL)
          $code_str = 'null';
        else
          $code_str = sprintf('%d', $exit_code);

        $message = sprintf('Task already completed with status %s (exit code %s).', $status, $code_str);
      }
    }
    else {
      // Just checking the task status
      $message = 'Task found';
    }
  }
  else {
    $data = array();
    $message = 'Task not found';
  }

  $db->query('UNLOCK TABLES');
  
  send_response(200, 'OK', $message, array($data), 'json');

}


?>
