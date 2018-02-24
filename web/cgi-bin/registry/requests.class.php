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

  public function __construct($cert_dn, $issuer_dn, $service, $as_user = NULL)
  {
    global $db_conf;

    $this->_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamoregister');

    $sid = 0;
    $authorized = get_user($this->_db, $cert_dn, $issuer_dn, $service, $as_user, $this->_uid, $this->_uname, $sid);

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
    $this->lock_table('transfer');

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

      if ($request_id != 0) {
        $new_transfer = $this->get_transfer_data(array('request_id' => $request_id));
        $this->send_response(200, 'OK', 'Transfer requested', array_values($new_transfer));
      }
      else
        $this->send_response(400, 'InternalError', 'Failed to request transfer');
    }
    else {
      // known transfer - update
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
    $this->lock_table('deletion');

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
      $existing_data = $this->get_transfer_data($request, true);
    else if ($request['type'] == 'deletion')
      $existing_data = $this->get_deletion_data($request, true);
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

  private function get_transfer_data($request, $add_user = false)
  {
    // structure:
    // array(
    //   id => array('request_id' => id, 'item' => name, ...),
    //   ...
    // )

    $data = array();

    $fields = array(
      '`transfer_requests`.`id`',
      '`transfer_requests`.`item`',
      '`transfer_requests`.`site`',
      '`transfer_requests`.`group`',
      '`transfer_requests`.`num_copies`',
      '`transfer_requests`.`first_request_time`',
      '`transfer_requests`.`last_request_time`',
      '`transfer_requests`.`request_count`',
      '`active_transfers`.`site`',
      '`active_transfers`.`status`',
      '`active_transfers`.`updated`'
    );
    if ($add_user)
      $fields[] = '`users`.`name`';

    $query = 'SELECT ' . implode(',', $fields) . ' FROM `transfer_requests`';
    $query .= ' LEFT JOIN `active_transfers` ON `active_transfers`.`request_id` = `transfer_requests`.`id`';
    if ($add_user)
      $query .= ' INNER JOIN `users` ON `users`.`id` = `transfer_requests`.`user_id`';

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

    $query .= ' ORDER BY `transfer_requests`.`id`';

    $stmt = $this->_db->prepare($query);

    error_log($this->_db->error);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    if ($add_user)
      $stmt->bind_result($rid, $item, $site, $group, $ncopy, $first_request, $last_request, $request_count, $asite, $astatus, $atimestamp, $uname);
    else
      $stmt->bind_result($rid, $item, $site, $group, $ncopy, $first_request, $last_request, $request_count, $asite, $astatus, $atimestamp);

    $stmt->execute();

    $_rid = 0;
    while ($stmt->fetch()) {
      error_log($rid . $item);

      if ($rid != $_rid) {
        $_rid = $rid;

        $data[$rid] = array(
          'request_id' => $rid,
          'item' => $item,
          'site' => $site,
          'group' => $group,
          'ncopy' => $ncopy,
          'first_request' => $first_request,
          'last_request' => $last_request,
          'request_count' => $request_count
        );

        $datum = &$data[$rid];

        if ($add_user)
          $datum['user'] = $uname;

        $datum['transfer'] = array();
      }

      if ($astatus !== NULL)
        $datum['transfer'][] = array('site' => $asite, 'status' => $astatus, 'updated' => $atimestamp);
    }

    $stmt->close();

    return $data;
  }

  private function create_transfer_request($item, $site, $group, $ncopy)
  {
    $query = 'INSERT INTO `transfer_requests` (`item`, `site`, `group`, `num_copies`, `user_id`, `first_request_time`, `last_request_time`) VALUES (?, ?, ?, ?, ?, NOW(), NOW())';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('sssii', $item, $site, $group, $ncopy, $this->_uid);
    $stmt->execute();
    $request_id = $stmt->insert_id;
    $stmt->close();

    return $request_id;
  }

  private function update_transfer_request($request_ids, $content)
  {
    $query = 'UPDATE `transfer_requests` SET `last_request_time` = NOW(), `request_count` = `request_count` + 1, ';
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

  private function get_deletion_data($request, $add_user = false)
  {
    // structure:
    // array(
    //   id => array('request_id' => id, 'item' => name, ...),
    //   ...
    // )

    $data = array();

    $fields = array(
      '`deletion_requests`.`id`',
      '`deletion_requests`.`item`',
      '`deletion_requests`.`site`',
      '`deletion_requests`.`timestamp`',
      '`active_deletions`.`status`',
      '`active_deletions`.`updated`'
    );
    if ($add_user)
      $fields[] = '`users`.`name`';
    
    $query = 'SELECT ' . implode(',', $fields) . ' FROM `deletion_requests`';
    $query .= ' LEFT JOIN `active_deletions` ON `active_deletions`.`request_id` = `deletion_requests`.`id`';
    if ($add_user)
      $query .= ' INNER JOIN `users` ON `users`.`id` = `deletion_requests`.`user_id`';

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

    $query .= ' ORDER BY `deletion_requests`.`id`';

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    if ($add_user)
      $stmt->bind_result($rid, $item, $site, $timestamp, $astatus, $atimestamp, $uname);
    else
      $stmt->bind_result($rid, $item, $site, $timestamp, $astatus, $atimestamp);

    $stmt->execute();
    $_rid = 0;
    while ($stmt->fetch()) {
      if ($rid != $_rid) {
        $_rid = $rid;

        $data[$rid] = array(
          'request_id' => $rid,
          'item' => $item,
          'site' => $site,
          'requested' => $timestamp
        );

        $datum = &$data[$rid];

        if ($add_user)
          $datum['user'] = $uname;

        $datum['deletion'] = array();
      }

      if ($astatus === NULL)
        $datum['deletion'][] = array('status' => $astatus, 'updated' => $atimestamp);
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

  private function lock_table($operation = '')
  {
    if ($operation == 'transfer')
      $query = 'LOCK TABLES `transfer_requests` WRITE, `active_transfers` WRITE';
    else if ($operation == 'deletion')
      $query = 'LOCK TABLES `deletion_requests` WRITE, `active_deletions` WRITE';
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
