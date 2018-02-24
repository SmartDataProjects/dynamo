<?php

include_once(__DIR__ . '/../dynamo/common/db_conf.php');
include_once(__DIR__ . '/common.php');

// General note:
// MySQL DATETIME accepts and returns local time. Always use UNIX_TIMESTAMP and FROM_UNIXTIME to interact with the DB.

class Requests {
  public $format = 'json';
  public $return_data = true;

  private $_db = NULL;
  private $_uid = 0;
  private $_uname = '';
  private $_read_only = true;

  public function __construct($cert_dn, $issuer_dn, $as_user = NULL)
  {
    global $db_conf;

    $this->_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

    $sid = 0;
    $authorized = get_user($this->_db, $cert_dn, $issuer_dn, 'user', $as_user, $this->_uid, $this->_uname, $sid);

    if ($this->_uid == 0 || $sid == 0)
      $this->send_response(400, 'BadRequest', 'Unknown user');

    $this->_read_only = !$authorized;
  }

  public function execute($command, $request)
  {
    if (!in_array($command, array('transfer', 'delete', 'poll')))
      $this->send_response(400, 'BadRequest', 'Invalid command (possible values: transfer, delete, list)');

    if ($command != 'poll' && $this->_read_only)
      $this->send_response(400, 'BadRequest', 'User not authorized');

    $this->sanitize_request($command, $request);

    if ($command == 'transfer') {
      $this->exec_transfer($request);
    }
    else if ($command == 'delete') {
      $this->exec_delete($request);
    }
    else if ($command == 'poll') {
      $this->exec_poll($request);
    }
  }

  private function exec_transfer($request)
  {
    // we need the table to be consistent between get_data and insert
    $this->lock_table(true);

    $existing_data = $this->get_transfer_data($request);
    
    if (count($existing_data) == 0) {
      // this is a new transfer
      if (!isset($request['item']))
        $this->send_response(400, 'BadRequest', 'Item not given');
      if (!isset($request['site']))
        $this->send_response(400, 'BadRequest', 'Site or site group not given');
      if (!isset($request['group']))
        $this->send_response(400, 'BadRequest', 'Group not given');

      $ncopy = isset($request['ncopy']) ? $request['ncopy'] : 1;

      $request_id = $this->create_transfer_request($request['item'], $request['site'], $request['group'], $ncopy);

      if ($request_id != 0)
        $this->send_response(200, 'OK', 'Transfer requested', array_values($this->get_transfer_data(array('request_id' => $request_id))));
      else
        $this->send_response(400, 'InternalError', 'Failed to request transfer');
    }
    else {
      // known lock - update
      $updates = array();
      if (isset($request['group']))
        $updates['group'] = $request['group'];
      if (isset($request['ncopy']))
        $updates['ncopy'] = $request['ncopy'];

      if (count($updates) != 0) {
        $request_ids = array_keys($existing_data);

        $updated_data = $this->update_transfer_request($request_ids, $updates);
        if (is_array($updated_data))
          $this->send_response(200, 'OK', 'Request updated', array_values($updated_data));
        else
          $this->send_response(400, 'InternalError', 'Failed to update request');
      }
      else
        $this->send_response(200, 'OK', 'Request exists', array_values($existing_data));
    }
  }

  private function exec_delete($request)
  {
    // we need the table to be consistent between get_deletion_data and insert
    $this->lock_table(true);

    $existing_data = $this->get_deletion_data($request);
    
    if (count($existing_data) == 0) {
      // this is a new deletion
      if (!isset($request['item']))
        $this->send_response(400, 'BadRequest', 'Item not given');
      if (!isset($request['site']))
        $this->send_response(400, 'BadRequest', 'Site or site group not given');

      $request_id = $this->create_deletion_request($request['item'], $request['site']);

      if ($request_id != 0)
        $this->send_response(200, 'OK', 'Deletion requested', array_values($this->get_deletion_data(array('request_id' => $request_id))));
      else
        $this->send_response(400, 'InternalError', 'Failed to request deletion');
    }
    else
      $this->send_response(200, 'OK', 'Request exists', array_values($existing_data));
  }

  private function exec_poll($request)
  {
    if (!isset($request['type']))
      $this->send_response(400, 'BadRequest', 'Request type not given');

    if ($request['type'] == 'transfer')
      $existing_data = $this->get_transfer_data($request);
    else if ($request['type'] == 'deletion')
      $existing_data = $this->get_deletion_data($request);
    else
      $this->send_response(400, 'BadRequest', 'Invalid request type');

    if (count($existing_data) == 0)
      $this->send_response(200, 'EmptyResult', 'Request not found');
    else
      $this->send_response(200, 'OK', count($existing_data) . ' request(s) found', array_values($existing_data));
  }

  private function sanitize_request($command, &$request)
  {
    $allowed_fields = array('request_id', 'item', 'site');

    if ($command == 'transfer')
      $allowed_fields = array_merge($allowed_fields, array('group', 'ncopy'));
    else if ($command == 'poll')
      $allowed_fields = array_merge($allowed_fields, array('type'));

    foreach (array_keys($request) as $key) {
      if (in_array($key, $allowed_fields)) {
        if ($key == 'request_id' || $key == 'ncopy')
          $request[$key] = 0 + $request[$key];
        else
          $request[$key] = $this->_db->real_escape_string($request[$key]);
      }
      else
        $this->send_response(400, 'BadRequest', 'Field "' . $key . '" not allowed for operation "' . $command . '".');
    }
  }

  private function transfer_has_diff($update, $ref)
  {
    if (isset($update['group']) && $update['group'] != $ref['group'])
      return true;

    if (isset($update['ncopy']) && $update['ncopy'] != $ref['ncopy'])
      return true;

    return false;
  }

  private function get_transfer_data($request)
  {
    // structure:
    // array(
    //   id => array('request_id' => id, 'item' => name, ...),
    //   ...
    // )

    $data = array();

    $query = 'SELECT `transfer_requests`.`id`, `transfer_requests`.`item`, `transfer_requests`.`site`, `transfer_requests`.`group`,';
    $query .= ' `transfer_requests`.`num_copies`, `transfer_requests`.`timestamp`,';
    $query .= ' `users`.`name`, `active_transfers`.`site`, `active_transfers`.`status`, `active_transfers`.`updated` FROM `transfer_requests`';
    $query .= ' INNER JOIN `users` ON `users`.`id` = `transfer_requests`.`user_id`';
    $query .= ' LEFT JOIN `active_transfers` ON `active_transfers`.`request_id` = `transfer_requests`.`id`';

    $where_clause = array();
    $params = array('');

    if (isset($request['request_id'])) {
      $where_clause[] = '`transfer_requests`.`id` = ?';
      $params[0] .= 'i';
      $params[] = &$request['request_id'];
    }
    else {
      if (isset($request['item'])) {
        $where_clause[] =  '`transfer_requests`.`item` = ?';
        $params[0] .= 's';
        $params[] = &$request['item'];
      }

      if (isset($request['site'])) {
        $where_clause[] =  '`transfer_requests`.`site` = ?';
        $params[0] .= 's';
        $params[] = &$request['site'];
      }

      if (isset($request['group'])) {
        $where_clause[] =  '`transfer_requests`.`group` LIKE ?';
        $params[0] .= 's';
        $params[] = &$request['group'];
      }
    }

    if (count($where_clause) != 0)
      $query .= ' WHERE ' . implode(' AND ', $where_clause);

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    $stmt->bind_result($rid, $item, $site, $group, $ncopy, $timestamp, $uname, $asite, $astatus, $atimestamp);
    $stmt->execute();
    while ($stmt->fetch()) {
      $datum =
        array(
              'request_id' => $rid,
              'user' => $uname,
              'item' => $item,
              'site' => $site,
              'group' => $group,
              'ncopy' => $ncopy,
              'requested' => $timestamp
              );

      if ($astatus === NULL) {
        $datum['status'] = 'undefined';
        $datum['updated'] = $timestamp;
      }
      else {
        $datum['status'] = $astatus;
        $datum['updated'] = $atimestamp;
      }

      $data[$rid] = $datum;
    }

    $stmt->close();

    return $data;
  }

  private function create_transfer_request($item, $site, $group, $ncopy)
  {
    $query = 'INSERT INTO `transfer_requests` (`item`, `site`, `group`, `num_copies`, `user_id`, `timestamp`) VALUES (?, ?, ?, ?, ?, NOW())';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('sssii', $item, $site, $group, $ncopy, $this->_uid);
    $stmt->execute();
    $request_id = $stmt->insert_id;
    $stmt->close();

    return $request_id;
  }

  private function get_deletion_data($request)
  {
    // structure:
    // array(
    //   id => array('request_id' => id, 'item' => name, ...),
    //   ...
    // )

    $data = array();

    $query = 'SELECT `deletion_requests`.`id`, `deletion_requests`.`item`, `deletion_requests`.`site`, `deletion_requests`.`timestamp`,';
    $query .= ' `users`.`name`, `active_deletions`.`status`, `active_deletions`.`updated` FROM `deletion_requests`';
    $query .= ' INNER JOIN `users` ON `users`.`id` = `deletion_requests`.`user_id`';
    $query .= ' LEFT JOIN `active_deletions` ON `active_deletions`.`request_id` = `deletion_requests`.`id`';

    $where_clause = array();
    $params = array('');

    if (isset($request['request_id'])) {
      $where_clause[] = '`deletion_requests`.`id` = ?';
      $params[0] .= 'i';
      $params[] = &$request['request_id'];
    }
    else {
      if (isset($request['item'])) {
        $where_clause[] =  '`deletion_requests`.`item` = ?';
        $params[0] .= 's';
        $params[] = &$request['item'];
      }

      if (isset($request['site'])) {
        $where_clause[] =  '`deletion_requests`.`site` = ?';
        $params[0] .= 's';
        $params[] = &$request['site'];
      }
    }

    if (count($where_clause) != 0)
      $query .= ' WHERE ' . implode(' AND ', $where_clause);

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    $stmt->bind_result($rid, $item, $site, $timestamp, $uname, $astatus, $atimestamp);
    $stmt->execute();
    while ($stmt->fetch()) {
      $datum =
        array(
              'request_id' => $rid,
              'user' => $uname,
              'item' => $item,
              'site' => $site,
              'requested' => $timestamp
              );

      if ($astatus === NULL) {
        $datum['status'] = 'undefined';
        $datum['updated'] = $timestamp;
      }
      else {
        $datum['status'] = $astatus;
        $datum['updated'] = $atimestamp;
      }

      $data[$rid] = $datum;
    }

    $stmt->close();

    return $data;
  }

  private function create_deletion_request($item, $site)
  {
    $query = 'INSERT INTO `deletion_requests` (`item`, `site`, `user_id`, `timestamp`) VALUES (?, ?, ?, NOW())';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('ssi', $item, $site, $this->_uid);
    $stmt->execute();
    $request_id = $stmt->insert_id;
    $stmt->close();

    return $request_id;
  }

  private function update_transfer_request($request_ids, $content)
  {
    $query = 'UPDATE `transfer_requests` SET ';
    $params = array('');

    if (isset($content['group'])) {
      $set[] = '`group` = ?';
      $params[0] .= 's';
      $params[] = &$content['group'];
    }

    if (isset($content['ncopy'])) {
      $set[] = '`num_copies` = ?';
      $params[0] .= 'i';
      $params[] = &$content['ncopy'];
    }

    $query .= implode(', ', $set);

    if (is_array($request_ids))
      $query .= ' WHERE `id` IN (' . implode(',', $request_ids) . ')';
    else
      $query .= ' WHERE `id` = ' . $request_ids;

    $stmt = $this->_db->prepare($query);
    call_user_func_array(array($stmt, "bind_param"), $params);
    $stmt->execute();
    $stmt->close();

    if (is_array($request_ids)) {
      $data = array();
      foreach ($request_ids as $rid)
        $data = array_merge($data, $this->get_transfer_data(array('request_id' => $rid)));
    }
    else
      $data = $this->get_transfer_data(array('request_id' => $rid));

    // validate
    foreach ($data as $datum) {
      if ($this->transfer_has_diff($content, $datum))
        return NULL;
    }

    return $data;
  }

  private function lock_table($updating)
  {
    if ($updating) {
      $query = 'LOCK TABLES `transfer_requests` WRITE, `deletion_requests` WRITE,';
      $query .= '`active_transfers` WRITE, `active_deletions` WRITE,';
      $query .= '`users` WRITE';
    }
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
