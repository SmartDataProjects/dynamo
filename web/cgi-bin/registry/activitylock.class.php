<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

// General note:
// MySQL DATETIME accepts and returns local time. Always use UNIX_TIMESTAMP and FROM_UNIXTIME to interact with the DB.

class ActivityLock {
  private $_db = NULL;
  private $_uid = 0;
  private $_sid = 0;
  private $_apps = array();

  public function __construct()
  {
    global $db_conf;

    // get the list of application names from information_schema
    $info_schema = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'information_schema');
    $query = 'SELECT `COLUMN_TYPE` FROM `COLUMNS` WHERE `TABLE_SCHEMA` = "dynamoregister" AND `TABLE_NAME` = "activity_lock" AND `COLUMN_NAME` = "application"';
    $stmt = $info_schema->prepare($query);
    $stmt->bind_result($types);
    $stmt->execute();
    $stmt->fetch();
    $stmt->close();

    foreach (explode(',', substr($types, 5, strlen($types) - 6)) as $quoted)
      $this->_apps[] = trim($quoted, " '\"");

    $this->_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');
  }

  public function set_user($cert_dn, $issuer_dn, $service, $as_user = NULL)
  {
    get_user($this->_db, $cert_dn, $issuer_dn, $service, $as_user, $this->_uid, $this->_sid);

    if ($this->_uid == 0 || $this->_sid == 0)
      send_response(400, 'BadRequest', 'Unknown user');
  }

  public function execute($command, $request)
  {
    $this->sanitize_request($command, $request);

    if ($command == 'check') {
      $this->exec_check($request);
    }
    else if ($command == 'lock') {
      $this->exec_lock($request);
    }
    else if ($command == 'unlock') {
      $this->exec_unlock($request);
    }
  }

  private function exec_check($request)
  {
    $data = array();
    $active = $this->get_data($request['app'], $data);
    if ($active)
      send_response(200, 'OK', 'Lock active', array($data));
    else
      send_response(200, 'OK', 'No activity');
  }

  private function exec_lock($request)
  {
    $this->_db->query('LOCK TABLES `activity_lock`, `users`, `services` WRITE');

    $existing = array();

    if ($this->get_data($request['app'], $existing))
      send_response(200, 'OK', 'Failed: application already locked', array($existing));
    else {
      $note = isset($request['note']) ? $request['note'] : NULL;

      $query = 'INSERT INTO `activity_lock` (`user_id`, `service_id`, `application`, `timestamp`, `note`) VALUES (?, ?, ?, NOW(), ?)';
      $stmt = $this->_db->prepare($query);
      $stmt->bind_param('iiss', $this->_uid, $this->_sid, $request['app'], $note);
      $stmt->execute();
      $stmt->close();

      $data = array();

      if ($this->get_data($request['app'], $data))
        send_response(200, 'OK', 'OK: Lock active', array($data));
      else
        send_response(400, 'InternalError', 'Failed to lock ' . $request['app']);
    }
  }

  private function exec_unlock($request)
  {
    $this->_db->query('LOCK TABLES `activity_lock` WRITE');

    $existing = array();

    if (!$this->get_data($request['app'], $existing, true))
      send_response(200, 'OK', 'OK: application already unlocked');

    if ($existing['uid'] == $this->_uid && $existing['sid'] == $this->_sid) {
      $query = 'DELETE FROM `activity_lock` WHERE `user_id` = ? AND `service_id` = ? AND `application` = ?';
      $stmt = $this->_db->prepare($query);
      $stmt->bind_param('iis', $this->_uid, $this->_sid, $request['app']);
      $stmt->execute();
      $stmt->close();

      send_response(200, 'OK', 'OK: Lock disactivated');
    }
    else
      send_response(200, 'OK', 'Failed: application locked by another user');
  }

  private function sanitize_request($command, &$request)
  {
    $allowed_fields = array('app');

    if ($command == 'lock')
      $allowed_fields[] = 'note';

    foreach (array_keys($request) as $key) {
      if (in_array($key, $allowed_fields)) {
        $request[$key] = $this->_db->real_escape_string($request[$key]);
      }
      else
        unset($request[$key]);
    }

    if (!isset($request['app']))
      send_response(400, 'BadRequest', 'No app given');
    else if(!in_array($request['app'], $this->_apps))
      send_response(400, 'BadRequest', 'Unknown app');
  }

  private function get_data($application, &$data, $numerical = false)
  {
    if ($numerical) {
      $query = 'SELECT `user_id`, `service_id`, `timestamp`, `note` FROM `activity_lock`';
      $query .= ' WHERE `application` = ?';
    }
    else {
      $query = 'SELECT `users`.`name`, `services`.`name`, `activity_lock`.`timestamp`, `activity_lock`.`note` FROM `activity_lock`';
      $query .= ' INNER JOIN `users` ON `users`.`id` = `activity_lock`.`user_id`';
      $query .= ' INNER JOIN `services` ON `services`.`id` = `activity_lock`.`service_id`';
      $query .= ' WHERE `activity_lock`.`application` = ?';
    }

    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $application);
    $stmt->bind_result($uname,  $sname, $timestamp, $note);
    $stmt->execute();
    $active = $stmt->fetch();
    $stmt->close();

    if ($numerical) {
      $data['uid'] = $uname;
      $data['sid'] = $sname;
    }
    else {
      $data['user'] = $uname;
      if ($sname != 'user')
        $data['service'] = $sname;
    }

    $data['timestamp'] = $timestamp;
    if ($note !== NULL)
      $data['note'] = $note;
    
    return $active;
  }
}

?>
