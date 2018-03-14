<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

// General note:
// MySQL DATETIME accepts and returns local time. Always use UNIX_TIMESTAMP and FROM_UNIXTIME to interact with the DB.

class ActivityLock {
  private $_db = NULL;
  private $_uid = 0;
  private $_uname = '';
  private $_sid = 0;
  private $_sname = '';
  private $_read_only = true;
  private $_apps = array();

  public function __construct($cert_dn, $issuer_dn, $service, $as_user = NULL)
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

    $authorized = get_user($this->_db, $cert_dn, $issuer_dn, $service, $as_user, $this->_uid, $this->_uname, $this->_sid);

    if ($this->_uid == 0 || $this->_sid == 0)
      $this->send_response(400, 'BadRequest', 'Unknown user');

    $this->_read_only = !$authorized;
  }

  public function execute($command, $request)
  {
    if (!in_array($command, array('check', 'lock', 'unlock')))
      $this->send_response(400, 'BadRequest', 'Invalid command (possible values: check, lock, unlock)');

    if ($command != 'check' && $this->_read_only)
      $this->send_response(400, 'BadRequest', 'User not authorized');

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
    $data = $this->get_lock($request['app'], $first_uid, $first_sid);

    if ($first_uid == 0 && $first_sid == 0)
      $this->send_response(200, 'OK', 'Not locked');
    else
      $this->send_response(200, 'OK', 'Locked', array($data));
  }

  private function exec_lock($request)
  {
    $query = 'INSERT INTO `activity_lock` (`user_id`, `service_id`, `application`, `timestamp`, `note`) VALUES (?, ?, ?, NOW(), ?)';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('iiss', $this->_uid, $this->_sid, $request['app'], $request['note']);
    $stmt->execute();
    $stmt->close();

    $data = $this->get_lock($request['app'], $first_uid, $first_sid);

    if ($first_uid == $this->_uid && $first_sid == $this->_sid)
      $this->send_response(200, 'OK', 'Locked', array($data));
    else
      $this->send_response(200, 'WAIT', sprintf('Locked by %s', $data['user']), array($data));
  }

  private function exec_unlock($request)
  {
    // need to nest subqueries two levels to avoid MySQL error 1093
    $query = 'DELETE FROM `activity_lock` WHERE `user_id` = ? AND `service_id` = ? AND `application` = ? AND `timestamp` = (';
    $query .= 'SELECT x.t FROM (';
    $query .= 'SELECT MAX(`timestamp`) AS t FROM `activity_lock` WHERE `user_id` = ? AND `service_id` = ? AND `application` = ?';
    $query .= ') AS x) LIMIT 1';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('iisiis', $this->_uid, $this->_sid, $request['app'], $this->_uid, $this->_sid, $request['app']);
    $stmt->execute();
    $stmt->close();

    $data = $this->get_lock($request['app'], $first_uid, $first_sid);

    if ($first_uid == 0 && $first_sid == 0)
      $this->send_response(200, 'OK', 'Unlocked');
    else if ($first_uid == $this->_uid && $first_sid == $this->_sid)
      $this->send_response(200, 'OK', 'Locked', array($data));
    else
      $this->send_response(200, 'WAIT', sprintf('Locked by %s', $data['user']), array($data));
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
      $this->send_response(400, 'BadRequest', 'No app given');
    else if(!in_array($request['app'], $this->_apps))
      $this->send_response(400, 'BadRequest', 'Unknown app');
  }
  
  private function get_lock($app, &$first_uid, &$first_sid)
  {
    $query = 'SELECT u.`id`, s.`id`, u.`name`, s.`name`, l.`timestamp`, l.`note` FROM `activity_lock` AS l';
    $query .= ' INNER JOIN `users` AS u ON u.`id` = l.`user_id`';
    $query .= ' INNER JOIN `services` AS s ON s.`id` = l.`service_id`';
    $query .= ' WHERE l.`application` = ?';
    $query .= ' ORDER BY l.`timestamp` ASC';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $app);
    $stmt->bind_result($uid, $sid, $uname, $sname, $timestamp, $note);
    $stmt->execute();

    $data = array();

    if (!$stmt->fetch()) {
      $stmt->close();

      $first_uid = 0;
      $first_sid = 0;
      return $data;
    }

    $first_uid = $uid;
    $first_sid = $sid;

    $data['user'] = $uname;
    if ($sname != 'user')
      $data['service'] = $sname;
    $data['lock_time'] = $timestamp;
    if ($note !== NULL)
      $data['note'] = $note;

    $data['depth'] = 1;
    while ($stmt->fetch()) {
      if ($uid == $first_uid && $sid == $first_sid)
        ++$data['depth'];
    }

    $stmt->close();

    return $data;
  }

  private function send_response($code, $result, $message, $data = NULL)
  {
    // Table lock will be released at the end of the session. Explicit unlocking is in principle unnecessary.
    send_response($code, $result, $message, $data);
  }
}

?>
