<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

// General note:
// MySQL DATETIME accepts and returns local time. Always use UNIX_TIMESTAMP and FROM_UNIXTIME to interact with the DB.

class DetoxLock {
  public $format = 'json';
  public $return_data = true;

  private $_db = NULL;
  private $_uid = 0;
  private $_sid = 0;
  private $_read_only = true;

  public function __construct($cert_dn, $issuer_dn, $service, $as_user = NULL)
  {
    global $db_conf;

    $this->_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

    $authorized = get_user($this->_db, $cert_dn, $issuer_dn, $service, $as_user, $this->_uid, $this->_sid);

    if ($this->_uid == 0 || $this->_sid == 0)
      $this->send_response(400, 'BadRequest', 'Unknown user');

    $this->_read_only = !$authorized;
  }

  public function execute($command, $request)
  {
    if (!in_array($command, array('lock', 'unlock', 'list', 'set')))
      $this->send_response(400, 'BadRequest', 'Invalid command (possible values: lock, unlock, list, set)');

    if ($command != 'list' && $this->_read_only)
      $this->send_response(400, 'BadRequest', 'User not authorized');

    $this->sanitize_request($command, $request);

    if ($command == 'lock') {
      $this->exec_lock($request);
    }
    else if ($command == 'unlock') {
      $this->exec_unlock($request);
    }
    else if ($command == 'list') {
      $this->exec_list($request);
    }
    else if ($command == 'set') {
      $this->exec_set();
    }
  }

  private function exec_lock($request)
  {
    // we need the table to be consistent between get_data and create_lock/update_lock
    $this->lock_table(true);

    $existing_data = $this->get_data($request);
    
    if (count($existing_data) == 0) {
      // this is a new lock
      if (!isset($request['item']))
        $this->send_response(400, 'BadRequest', 'Item not given');
      if (!isset($request['expires']))
        $this->send_response(400, 'BadRequest', 'Expiration date not given');

      $sites = isset($request['sites']) ? $request['sites'] : NULL;
      $groups = isset($request['groups']) ? $request['groups'] : NULL;
      $comment = isset($request['comment']) ? $request['comment'] : NULL;

      $lockid = $this->create_lock($request['item'], $sites, $groups, $request['expires'], $comment);

      if ($lockid != 0)
        $this->send_response(200, 'OK', 'Lock created', array_values($this->get_data(array('lockid' => $lockid))));
      else
        $this->send_response(400, 'InternalError', 'Failed to create lock');
    }
    else {
      // known lock - update
      $updates = array();
      if (isset($request['expires']))
        $updates['expires'] = $request['expires'];
      if (isset($request['comment']))
        $updates['comment'] = $request['comment'];

      if (count($updates) != 0) {
        $lockids = array_keys($existing_data);

        $updated_data = $this->update_lock($lockids, $updates);
        if (is_array($updated_data))
          $this->send_response(200, 'OK', 'Lock updated', array_values($updated_data));
        else
          $this->send_response(400, 'InternalError', 'Failed to update lock');
      }
      else
        $this->send_response(200, 'OK', 'Lock exists', array_values($existing_data));
    }
  }

  private function exec_unlock($request)
  {
    $this->lock_table(true);

    $existing_data = $this->get_data($request);

    if (count($existing_data) == 0)
      $this->send_response(200, 'EmptyResult', 'No lock found');

    $lockids = array_keys($existing_data);

    $success = $this->disable_lock($lockids);

    if ($success)
      $this->send_response(200, 'OK', 'Lock disabled', array_values($this->get_data(array('lockid' => $lockids))));
    else
      $this->send_response(400, 'InternalError', 'Failed to update lock');
  }

  private function exec_list($request)
  {
    $skip_disabled = true;
    if (isset($request['showall'])) {
      if ($request['showall'] == 'y')
        $skip_disabled = false;
      else if ($request['showall'] == 'n')
        $skip_disabled = true;
      else
        $this->send_response(400, 'BadRequest', 'Only showall=y or showall=n allowed');
    }

    $uname = isset($request['user']) ? $request['user'] : NULL;

    $sname = isset($request['service']) ? $request['service'] : 'user';

    $existing_data = $this->get_data($request, $skip_disabled, $uname, $sname);

    if (count($existing_data) == 0)
      $this->send_response(200, 'EmptyResult', 'No lock found');
    else
      $this->send_response(200, 'OK', count($existing_data) . ' locks found', array_values($existing_data));
  }

  private function exec_set()
  {
    $input = file_get_contents('php://input');

    if ($this->format == 'json') {
      $data = json_decode($input, true);
      if (!is_array($data))
        $this->send_response(400, 'BadRequest', 'Invalid data posted');
    }
    else if ($this->format == 'xml') {
      $data = array();

      $reader = new XMLReader();
      if (!$reader->xml($input) || !$reader->read() || $reader->name != 'locks')
        $this->send_response(400, 'BadRequest', 'Invalid data posted');

      // structure:
      // <locks>
      //  <lock>
      //   <property>value</property>
      //   <property>value</property>
      //  </lock>
      // </locks>

      if (!$reader->read())
        $this->send_response(400, 'BadRequest', 'Invalid data posted');

      while (true) {
        // reader->name may be a lock name (no space between this tag and the previous closing) or #text
        if ($reader->name == '#text' && !$reader->read())
          $this->send_response(400, 'BadRequest', 'Invalid data posted');

        if ($reader->name == 'locks' && $reader->nodeType == XMLReader::END_ELEMENT) // </locks>
          break;
        else if ($reader->name != 'lock')
          $this->send_response(400, 'BadRequest', 'Invalid data posted');

        if (!$reader->read())
          $this->send_response(400, 'BadRequest', 'Invalid data posted');

        $entry = array();
      
        while (true) {
          if ($reader->name == '#text' && !$reader->read())
            $this->send_response(400, 'BadRequest', 'Invalid data posted');

          if ($reader->name == 'lock' && $reader->nodeType == XMLReader::END_ELEMENT) // </lock>
            break;

          $entry[$reader->name] = $reader->readInnerXML();

          if (!$reader->next())
            $this->send_response(400, 'BadRequest', 'Invalid data posted');
        }

        $data[] = $entry;

        if (!$reader->next())
          $this->send_response(400, 'BadRequest', 'Invalid data posted');
      }
    }

    // now we determine what's in the table already and what needs to be added
    $this->lock_table(true);

    $to_insert = array();
    $to_update = array();
    $to_unlock = array();

    foreach ($data as $key => $request) {
      $this->sanitize_request('lock', $request);

      $lockid = 0;

      if (isset($request['lockid'])) {
        // an update

        $current_locks = $this->get_data(array('lockid' => $request['lockid']));
        if (count($current_locks) == 0)
          $this->send_response(400, 'BadRequest', 'Lock id ' . $request['lockid'] . ' not found');

        $to_update[$request['lockid']] = $request;
      }
      else {
        if (!isset($request['item']))
          $this->send_response(400, 'BadRequest', 'Missing item name in entry ' . $key);

        $current_locks = $this->get_data($request);

        if (count($current_locks) == 0) {
          // new lock
          if (!isset($request['expires']))
            $this->send_response(400, 'BadRequest', 'Missing expiry date in entry ' . $key);

          $sites = isset($request['sites']) ? $request['sites'] : NULL;
          $groups = isset($request['groups']) ? $request['groups'] : NULL;
          $comment = isset($request['comment']) ? $request['comment'] : NULL;

          $to_insert[] = array($request['item'], $sites, $groups, $request['expires'], $comment);
        }
        else {
          $lockid = current(array_keys($current_locks));
          $to_update[$lockid] = $request;
        }
      }
    }

    $existing_entries = $this->get_data();

    foreach (array_keys($existing_entries) as $key) {
      if (!array_key_exists($key, $to_update))
        $to_unlock[] = $key;
    }

    $inserted_ids = array();
    foreach ($to_insert as $request) {
      $lockid = $this->create_lock($request[0], $request[1], $request[2], $request[3], $request[4]);
      if ($lockid == 0)
        $this->send_response(400, 'InternalError', 'Failed to create lock');

      $inserted_ids[] = $lockid;
    }

    foreach ($to_update as $lockid => $content) {
      $data = $this->update_lock($lockid, $content);
      if (count($data) == 0)
        $this->send_response(400, 'InternalError', 'Failed to update lock');
    }

    if (count($to_unlock) != 0) {
      if (!$this->disable_lock($to_unlock))
        $this->send_response(400, 'InternalError', 'Failed to disable lock');
    }

    $lockids = array_merge($inserted_ids, array_keys($to_update), $to_unlock);

    $this->send_response(200, 'OK', 'Locks set', array_values($this->get_data(array('lockid' => $lockids))));
  }

  private function sanitize_request($command, &$request)
  {
    $allowed_fields = array('lockid', 'item', 'sites', 'groups');

    if ($command == 'lock')
      $allowed_fields = array_merge($allowed_fields, array('expires', 'comment'));
    else if ($command == 'unlock')
      $allowed_fields = array_merge($allowed_fields, array('created_before', 'created_after', 'expires_before', 'expires_after'));
    else if ($command == 'list')
      $allowed_fields = array_merge($allowed_fields, array('created_before', 'created_after', 'expires_before', 'expires_after', 'showall', 'user', 'service'));

    foreach (array_keys($request) as $key) {
      if (in_array($key, $allowed_fields)) {
        if ($key == 'lockid')
          $request[$key] = 0 + $request[$key];
        else if (strpos($key, 'expires') === 0)
          $request[$key] = $this->format_timestamp($request[$key], true);
        else if (strpos($key, 'created') === 0)
          $request[$key] = $this->format_timestamp($request[$key]);
        else
          $request[$key] = $this->_db->real_escape_string($request[$key]);
      }
      else
        unset($request[$key]);
    }
  }

  private function get_data($request = array(), $skip_disabled = true, $uname = NULL, $sname = NULL)
  {
    $data = array();

    $query = 'SELECT `detox_locks`.`id`, `detox_locks`.`item`, `detox_locks`.`sites`, `detox_locks`.`groups`,';
    $query .= ' UNIX_TIMESTAMP(`detox_locks`.`lock_date`), UNIX_TIMESTAMP(`detox_locks`.`unlock_date`), UNIX_TIMESTAMP(`detox_locks`.`expiration_date`),';
    $query .= ' `users`.`name`, `services`.`name`, `detox_locks`.`comment` FROM `detox_locks`';
    $query .= ' INNER JOIN `users` ON `users`.`id` = `detox_locks`.`user_id`';
    $query .= ' INNER JOIN `services` ON `services`.`id` = `detox_locks`.`service_id`';

    $where_clause = array();
    $params = array('');

    if (isset($request['lockid'])) {
      if (is_array($request['lockid'])) {
        if (count($request['lockid']) == 0)
          return $data;

        $where_clause[] = '`detox_locks`.`id` IN (' . implode(',', $request['lockid']) . ')';
      }
      else if ($request['lockid'] > 0) {
        $where_clause[] = '`detox_locks`.`id` = ?';
        $params[0] .= 'i';
        $params[] = &$request['lockid'];
      }
      else
        $this->send_response(400, 'BadRequest', 'Invalid lock id ' . $request['lockid']);
    }
    else {
      if ($skip_disabled) {
        $where_clause[] =  '`detox_locks`.`unlock_date` IS NULL';
      }

      if ($uname === NULL) {
        $where_clause[] = '`users`.`id` = ?';
        $params[0] .= 'i';
        $params[] = &$this->_uid;
      }
      else {
        $where_clause[] = '`users`.`name` = ?';
        $params[0] .= 's';
        $params[] = &$uname;
      }

      if ($sname === NULL) {
        $where_clause[] = '`services`.`id` = ?';
        $params[0] .= 'i';
        $params[] = &$this->_sid;
      }
      else {
        $where_clause[] = '`services`.`name` = ?';
        $params[0] .= 's';
        $params[] = &$sname;
      }

      if (isset($request['item'])) {
        $where_clause[] =  '`detox_locks`.`item` LIKE ?';
        $params[0] .= 's';
        $params[] = &$request['item'];
      }

      if (isset($request['sites'])) {
        $where_clause[] =  '`detox_locks`.`sites` LIKE ?';
        $params[0] .= 's';
        $params[] = &$request['sites'];
      }

      if (isset($request['groups'])) {
        $where_clause[] =  '`detox_locks`.`groups` LIKE ?';
        $params[0] .= 's';
        $params[] = &$request['groups'];
      }

      if (isset($request['created_before'])) {
        $where_clause[] =  '`detox_locks`.`lock_date` <= FROM_UNIXTIME(?)';
        $params[0] .= 'i';
        $params[] = &$request['created_before'];
      }

      if (isset($request['created_after'])) {
        $where_clause[] =  '`detox_locks`.`lock_date` >= FROM_UNIXTIME(?)';
        $params[0] .= 'i';
        $params[] = &$request['created_after'];
      }

      if (isset($request['expires_before'])) {
        $where_clause[] =  '`detox_locks`.`expiration_date` <= FROM_UNIXTIME(?)';
        $params[0] .= 'i';
        $params[] = &$request['expires_before'];
      }

      if (isset($request['expires_after'])) {
        $where_clause[] =  '`detox_locks`.`expiration_date` >= FROM_UNIXTIME(?)';
        $params[0] .= 'i';
        $params[] = &$request['expires_after'];
      }
    }

    if (count($where_clause) != 0)
      $query .= ' WHERE ' . implode(' AND ', $where_clause);

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    $stmt->bind_result($lid, $item, $sites, $groups, $created, $disabled, $expiration, $uname, $sname, $comment);
    $stmt->execute();
    while ($stmt->fetch()) {
      $data[$lid] =
        array(
              'lockid' => $lid,
              'user' => $uname,
              'item' => $item,
              'locked' => strftime('%Y-%m-%d %H:%M:%S', $created),
              'expires' => strftime('%Y-%m-%d %H:%M:%S', $expiration),
              );

      if ($sname != 'user')
        $data[$lid]['service'] = $sname;
      if ($sites !== NULL)
        $data[$lid]['sites'] = $sites;
      if ($groups !== NULL)
        $data[$lid]['groups'] = $groups;
      if ($disabled !== NULL)
        $data[$lid]['unlocked'] = strftime('%Y-%m-%d %H:%M:%S', $disabled);
      if ($comment !== NULL)
        $data[$lid]['comment'] = $comment;
    }

    $stmt->close();

    return $data;
  }

  private function create_lock($item, $sites, $groups, $expiration, $comment)
  {
    $query = 'INSERT INTO `detox_locks` (`item`, `sites`, `groups`, `lock_date`, `expiration_date`, `user_id`, `service_id`, `comment`) VALUES (?, ?, ?, NOW(), FROM_UNIXTIME(?), ?, ?, ?)';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('ssssiis', $item, $sites, $groups, $expiration, $this->_uid, $this->_sid, $comment);
    $stmt->execute();
    $lockid = $stmt->insert_id;
    $stmt->close();

    return $lockid;
  }

  private function update_lock($lockids, $content)
  {
    $query = 'UPDATE `detox_locks` SET ';
    $params = array('');

    if (isset($content['expires'])) {
      $set[] = '`expiration_date` = FROM_UNIXTIME(?)';
      $params[0] .= 's';
      $params[] = &$content['expires'];
    }

    if (isset($content['comment'])) {
      $set[] = '`comment` = ?';
      $params[0] .= 's';
      $params[] = &$content['comment'];
    }

    $query .= implode(', ', $set);

    if (is_array($lockids))
      $query .= ' WHERE `id` IN (' . implode(',', $lockids) . ')';
    else
      $query .= ' WHERE `id` = ' . $lockids;

    $stmt = $this->_db->prepare($query);
    call_user_func_array(array($stmt, "bind_param"), $params);
    $stmt->execute();
    $stmt->close();

    $data = $this->get_data(array('lockid' => $lockids));

    // validate
    foreach ($data as $datum) {
      if ((isset($content['expires']) && strtotime($datum['expires']) != $content['expires']) ||
          (isset($content['comment']) && $datum['comment'] != $content['comment']))
        return NULL;
    }

    return $data;
  }

  private function disable_lock($lockids)
  {
    $query = 'UPDATE `detox_locks` SET `unlock_date` = NOW()';

    if (is_array($lockids))
      $query .= ' WHERE `id` IN (' . implode(',', $lockids) . ')';
    else
      $query .= ' WHERE `id` = ' . $lockids;

    $stmt = $this->_db->prepare($query);
    $stmt->execute();
    $stmt->close();

    return true;
  }

  private function format_timestamp($value, $must_be_in_future = false)
  {
    if (is_numeric($value))
      $timestamp = 0 + $value;
    else {
      $timestamp = strtotime($value);
      if ($timestamp === false)
        $this->send_response(400, 'BadRequest', 'Date string ' . $value . ' ill-formatted');
    }

    if ($must_be_in_future && $timestamp < time())
      $this->send_response(400, 'BadRequest', 'Date ' . $value . ' must be in the future');

    return $timestamp;
  }

  private function lock_table($updating)
  {
    if ($updating)
      $query = 'LOCK TABLES `detox_locks`, `users`, `services` WRITE';
    else
      $query = 'UNLOCK TABLES';

    $this->_db->query($query);
  }

  private function send_response($code, $result, $message, $data = NULL)
  {
    // Table lock will be released at the end of the session. Explicit unlocking is in principle unnecessary.
    $this->lock_table(false);
    send_response($code, $result, $message, $this->return_data ? $data : NULL, $this->format);
  }
}

?>
