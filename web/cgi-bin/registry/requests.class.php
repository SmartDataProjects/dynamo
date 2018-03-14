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
    if (!in_array($command, array('copy', 'delete', 'pollcopy', 'polldeletion', 'cancelcopy', 'canceldeletion')))
      $this->send_response(400, 'BadRequest', 'Invalid command (possible values: copy, delete, list)');

    if ($command != 'pollcopy' && $command != 'polldeletion' && $this->_read_only)
      $this->send_response(400, 'BadRequest', 'User not authorized');

    $this->sanitize_request($command, $request);

    if ($command == 'copy') {
      $this->exec_copy($request);
    }
    else if ($command == 'delete') {
      $this->exec_delete($request);
    }
    else if ($command == 'pollcopy') {
      $this->exec_poll($request, 'copy');
    }
    else if ($command == 'polldeletion') {
      $this->exec_poll($request, 'deletion');
    }
    else if ($command == 'cancelcopy') {
      $this->exec_cancel($request, 'copy');
    }
    else if ($command == 'canceldeletion') {
      $this->exec_cancel($request, 'deletion');
    }
  }

  private function exec_copy($request)
  {
    $input = file_get_contents('php://input');

    if ($input) {
      // json uploaded
      $request = json_decode($input, true);
      if (!is_array($request))
        $this->send_response(400, 'BadRequest', 'Invalid data posted');
    }

    // can update copy data using an existing request_id
    $request_id = isset($request['request_id']) ? $request['request_id'] : NULL;

    if (isset($request['item']))
      $items = $request['item'];
    else if ($request_id === NULL)
      $this->send_response(400, 'BadRequest', 'Item not given');
    else
      $items = NULL;

    if (isset($request['site']))
      $sites = $request['site'];
    else if ($request_id === NULL)
      $this->send_response(400, 'BadRequest', 'Site not given');
    else
      $sites = NULL;

    // we need the table to be consistent between get_data and insert
    $this->lock_table('copy');

    $existing_data = $this->get_copy_data($request_id, $items, $sites);

    if ($request_id !== NULL && count($existing_data) == 0)
      $this->send_response(400, 'BadRequest', 'Request not found');

    if (count($existing_data) == 0) {
      // this is a new copy

      $ncopy = isset($request['n']) ? $request['n'] : 1;

      $request_id = $this->create_copy_request($items, $sites, 'AnalysisOps', $ncopy);

      // need to unlock before using get_copy_data with user arg
      $this->lock_table(false);

      if ($request_id != 0) {
        $new_copy = $this->get_copy_data($request_id, NULL, NULL, $this->_uname);
        $this->send_response(200, 'OK', 'Copy requested', array_values($new_copy));
      }
      else
        $this->send_response(500, 'InternalError', 'Failed to request copy');
    }
    else {
      // known copy - update
      $updates = array();
      /* if (isset($request['group'])) */
      /*   $updates['group'] = $request['group']; */
      if (isset($request['n']))
        $updates['n'] = $request['n'];

      $updated_data = array();
      $request_ids = array_keys($existing_data);

      foreach ($request_ids as $request_id) {
        if ($existing_data[$request_id]['status'] != 'new')
          $updates['status'] = 'updated';
        else
          $updates['status'] = 'new';

        $this->update_copy_request($request_id, $updates);

        // need to unlock before using get_copy_data with user arg
        $this->lock_table(false);

        $data = $this->get_copy_data($request_id, NULL, NULL, $this->_uname);
        $updated_data[] = $data[$request_id];
      }

      $this->send_response(200, 'OK', 'Request updated', $updated_data);
    }
  }

  private function exec_delete($request)
  {
    if (isset($_SERVER['CONTENT_TYPE']) && strcasecmp(trim($_SERVER['CONTENT_TYPE']), 'application/json') == 0) {
      // json uploaded
      $input = file_get_contents('php://input');

      $request = json_decode($input, true);
      if (!is_array($request))
        $this->send_response(400, 'BadRequest', 'Invalid data posted');
    }

    $request_id = isset($request['request_id']) ? $request['request_id'] : NULL;

    if (isset($request['item']))
      $items = $request['item']; // already made into an array by sanitize_request
    else if ($request_id === NULL)
      $this->send_response(400, 'BadRequest', 'Item not given');
    else
      $items = NULL;

    if (isset($request['site']))
      $sites = $request['site']; // already made into an array by sanitize_request
    else if ($request_id === NULL)
      $this->send_response(400, 'BadRequest', 'Site not given');
    else
      $sites = NULL;

    // we need the table to be consistent between get_deletion_data and insert
    $this->lock_table('deletion');

    $existing_data = $this->get_deletion_data($request_id, $items, $sites);

    if ($request_id !== NULL && count($existing_data) == 0)
      $this->send_response(400, 'BadRequest', 'Request not found');
    
    if (count($existing_data) == 0) {
      // this is a new deletion

      $request_id = $this->create_deletion_request($items, $sites);

      if ($request_id != 0) {
        $new_deletion = $this->get_deletion_data($request_id, NULL, NULL, $this->_uname);
        $this->send_response(200, 'OK', 'Deletion requested', array_values($new_deletion));
      }
      else
        $this->send_response(400, 'InternalError', 'Failed to request deletion');
    }
    else
      $this->send_response(200, 'OK', 'Request exists', array_values($existing_data));
  }

  private function exec_poll($request, $type)
  {
    if (isset($request['request_id']))
      $request_id = $request['request_id'];
    else
      $request_id = NULL;

    if (isset($request['item']))
      $items = $request['item'];
    else
      $items = NULL;

    if (isset($request['site']))
      $sites = $request['site'];
    else
      $sites = NULL;

    if (isset($request['user']))
      $users = $request['user'];
    else
      $users = NULL;

    // don't allow an empty query
    if ($request_id === NULL && $items === NULL && $sites === NULL && $users === NULL)
      $this->send_response(400, 'BadRequest', 'Need request_id, item, site, or user');

    // but if other fields are given and users is empty, set it to all users
    if ($users === NULL)
      $users = array('*');

    if ($type == 'copy')
      $existing_data = $this->get_copy_data($request_id, $items, $sites, $users);
    else
      $existing_data = $this->get_deletion_data($request_id, $items, $sites, $users);

    if (count($existing_data) == 0)
      $this->send_response(200, 'EmptyResult', 'Request not found');
    else
      $this->send_response(200, 'OK', 'Request found', array_values($existing_data));
  }

  private function exec_cancel($request, $type)
  {
    if (!isset($request['request_id']))
      $this->send_response(400, 'BadRequest', 'No request id given');

    $request_id = $request['request_id'];

    // we need the table to be consistent between get_deletion_data and insert
    $this->lock_table($type);

    if ($type == 'copy')
      $existing_data = $this->get_copy_data($request_id);
    else if ($type == 'deletion')
      $existing_data = $this->get_deletion_data($request_id);

    if (count($existing_data) == 0)
      $this->send_response(400, 'BadRequest', 'Invalid request id');

    $current_status = $existing_data[$request_id]['status'];
    if ($current_status == 'cancelled')
      $this->send_response(200, 'OK', 'Request already cancelled', $existing_data[$request_id]);
    else if ($current_status == 'completed' || $current_status == 'rejected')
      $this->send_response(400, 'BadRequest', 'Cannot cancel completed or rejected requests', $existing_data[$request_id]);

    if ($type == 'copy') {
      $this->update_copy_request($request_id, array('status' => 'cancelled'));
      $this->lock_table(false);
      $updated_data = $this->get_copy_data($request_id, NULL, NULL, $this->_uname);
    }
    else if ($type == 'deletion') {
      $this->cancel_deletion_request($request_id);
      $this->lock_table(false);
      $updated_data = $this->get_deletion_data($request_id, NULL, NULL, $this->_uname);
    }

    $this->send_response(200, 'OK', 'Request cancelled', $updated_data[$request_id]);
  }

  private function sanitize_request($command, &$request)
  {
    $allowed_fields = array('request_id', 'item', 'site');

    if ($command == 'copy')
      $allowed_fields = array_merge($allowed_fields, array('group', 'n'));
    else if ($command == 'pollcopy' || $command == 'polldeletion')
      $allowed_fields = array_merge($allowed_fields, array('user'));

    foreach (array_keys($request) as $key) {
      if (in_array($key, $allowed_fields)) {
        $value = $request[$key];

        if ($key == 'request_id' || $key == 'n') {
          $request[$key] = 0 + $value;
          if ($request[$key] < 0)
            $this->send_response(400, 'BadRequest', 'Invalid value ' . $value . ' for ' . $key);
        }
        else if (is_array($value)) {
          foreach (array_keys($value) as $k)
            $request[$key][$k] = $this->_db->real_escape_string($value[$k]);
        }
        else
          $request[$key] = $this->_db->real_escape_string($value);

        if ($key == 'item') {
          if (is_string($value))
            $value = $request[$key] = explode(',', trim($value, ','));

          foreach ($value as $item) {
            if (!preg_match('/^\/[^\/]+\/[^\/]+\/[^\/]+(?:#.+|)$/', $item))
              $this->send_response(400, 'BadRequest', 'Invalid item name ' . $item);
          }
        }
        else if ($key == 'site' || $key == 'user') {
          if (is_string($value))
            $request[$key] = explode(',', trim($value, ','));
        }
      }
      else
        $this->send_response(400, 'BadRequest', 'Field "' . $key . '" not allowed for operation "' . $command . '".');
    }
  }

  private function get_copy_data($request_id, $items = NULL, $sites = NULL, $users = NULL)
  {
    // param
    //  request_id: integer
    //  items: array
    //  sites: array
    //  users: array
    //
    // Structure of the returned data:
    // array(
    //   id => array('request_id' => id, 'item' => array(name,), ...),
    //   ...
    // )

    $data = array();

    $fields = array(
      'r.`id`',
      'r.`group`',
      'r.`num_copies`',
      'r.`status`',
      'r.`first_request_time`',
      'r.`last_request_time`',
      'r.`request_count`',
      'r.`rejection_reason`',
      'a.`site`',
      'a.`status`',
      'a.`updated`'
    );
    if ($users !== NULL) {
      $fields[] = 'u.`name`';
      $fields[] = 'u.`dn`';
    }

    $query = 'SELECT ' . implode(',', $fields) . ' FROM `copy_requests` AS r';
    $query .= ' LEFT JOIN `active_copies` AS a ON a.`request_id` = r.`id`';
    if ($users !== NULL)
      $query .= ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`';

    $where_clause = array();
    $params = array('');

    if ($users === NULL) {
      $where_clause[] = 'r.`user_id` = ?';
      $params[0] .= 'i';
      $params[] = &$this->_uid;
    }

    if ($request_id !== NULL) {
      $where_clause[] = 'r.`id` = ?';
      $params[0] .= 'i';
      $params[] = &$request_id;
    }
    else {
      // limit to live requests
      $where_clause[] = 'r.`status` IN (\'new\', \'activated\', \'updated\')';

      if ($users !== NULL && !in_array('*', $users)) {
        $quoted = array();
        foreach ($users as $u)
          $quoted[] = sprintf('\'%s\'', $u);
        $ulist = '(' . implode(',', $quoted) . ')';

        $where_clause[] = '(u.`name` IN ' . $ulist . ' OR u.`dn` IN ' . $ulist . ')';
      }

      if ($sites !== NULL || $items !== NULL) {
        $this->get_ids_by_sites_items('copy', $sites, $items, $params, $where_clause);
        $where_clause[] = 'r.`id` IN (SELECT `id` FROM `copy_ids_tmp`)';
      }
    }

    if (count($where_clause) != 0)
      $query .= ' WHERE ' . implode(' AND ', $where_clause);

    $query .= ' ORDER BY r.`id`';

    error_log($query);

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    if ($users !== NULL)
      $stmt->bind_result($rid, $group, $ncopy, $status, $first_request, $last_request, $request_count, $reason, $asite, $astatus, $atimestamp, $uname, $udn);
    else
      $stmt->bind_result($rid, $group, $ncopy, $status, $first_request, $last_request, $request_count, $reason, $asite, $astatus, $atimestamp);

    $stmt->execute();

    $_rid = 0;
    while ($stmt->fetch()) {
      if ($rid != $_rid) {
        $_rid = $rid;

        $data[$rid] = array(
          'request_id' => $rid,
          'site' => array(),
          'item' => array(),
          'group' => $group,
          'n' => $ncopy,
          'status' => $status,
          'first_request' => $first_request,
          'last_request' => $last_request,
          'request_count' => $request_count
        );

        $datum = &$data[$rid];

        if ($users !== NULL) {
          $datum['user'] = $uname;
          $datum['dn'] = $udn;
        }

        if ($status == 'rejected')
          $datum['reason'] = $reason;
        else if ($status == 'activated' || $status == 'completed')
          $datum['copy'] = array();
      }

      if ($astatus !== NULL)
        $datum['copy'][] = array('site' => $asite, 'status' => $astatus, 'updated' => $atimestamp);
    }

    $stmt->close();

    if (count($data) == 0)
      return $data;

    // get the sites for these requests
    $query = 'SELECT s.`request_id`, s.`site` FROM `copy_request_sites` AS s WHERE s.`request_id` IN (';
    $query .= implode(',', array_keys($data));
    $query .= ')';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_result($rid, $site);
    $stmt->execute();

    while ($stmt->fetch())
      $data[$rid]['site'][] = $site;

    $stmt->close();

    // get the items for these requests
    $query = 'SELECT i.`request_id`, i.`item` FROM `copy_request_items` AS i WHERE i.`request_id` IN (';
    $query .= implode(',', array_keys($data));
    $query .= ')';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_result($rid, $item);
    $stmt->execute();

    while ($stmt->fetch())
      $data[$rid]['item'][] = $item;

    $stmt->close();

    return $data;
  }

  private function create_copy_request($items, $sites, $group, $ncopy)
  {
    // param
    //  items: array
    //  site: string
    //  group: string
    //  ncopy: integer

    $query = 'INSERT INTO `copy_requests` (`group`, `num_copies`, `user_id`, `first_request_time`, `last_request_time`) VALUES (?, ?, ?, NOW(), NOW())';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('sii', $group, $ncopy, $this->_uid);
    $stmt->execute();
    $request_id = $stmt->insert_id;
    $stmt->close();

    $query = sprintf('INSERT INTO `copy_request_sites` (`request_id`, `site`) VALUES (%d, ?)', $request_id);
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $site_name);
    foreach ($sites as $nm) {
      $site_name = $nm;
      $stmt->execute();
    }
    $stmt->close();

    $query = sprintf('INSERT INTO `copy_request_items` (`request_id`, `item`) VALUES (%d, ?)', $request_id);
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $item_name);
    foreach ($items as $nm) {
      $item_name = $nm;
      $stmt->execute();
    }
    $stmt->close();

    return $request_id;
  }

  private function update_copy_request($request_id, $content)
  {
    $query = 'UPDATE `copy_requests` SET `last_request_time` = NOW(), ';

    $params = array('');

    if (isset($content['group'])) {
      $set[] = '`group` = ?';
      $params[0] .= 's';
      $params[] = &$content['group'];
    }

    if (isset($content['n'])) {
      $set[] = '`num_copies` = IF(`site` LIKE \'%*%\', ?, 1)';
      $params[0] .= 'i';
      $params[] = &$content['n'];
    }

    $cancelled = false;

    if (isset($content['status'])) {
      $set[] = '`status` = ?';
      $params[0] .= 's';
      $params[] = &$content['status'];

      if ($content['status'] == 'cancelled')
        $cancelled = true;
    }

    if (!$cancelled)
      $query .= '`request_count` = `request_count` + 1, ';

    $query .= implode(', ', $set);

    $query .= sprintf(' WHERE `id` = %d', $request_id);

    $stmt = $this->_db->prepare($query);
    call_user_func_array(array($stmt, "bind_param"), $params);
    $stmt->execute();
    $stmt->close();

    if ($cancelled)
      $this->_db->query(sprintf('DELETE FROM `active_copies` WHERE `request_id` = %d', $request_id));
  }

  private function get_deletion_data($request_id, $items = NULL, $sites = NULL, $users = NULL)
  {
    // param
    //  request_id: integer
    //  items: array
    //  sites: array
    //  users: array
    //
    // Returned structure:
    // array(
    //   id => array('request_id' => id, 'item' => name, ...),
    //   ...
    // )

    $data = array();

    $fields = array(
      'r.`id`',
      'r.`status`',
      'r.`timestamp`',
      'r.`rejection_reason`',
      'a.`status`',
      'a.`updated`'
    );
    if ($users !== NULL) {
      $fields[] = 'u.`name`';
      $fields[] = 'u.`dn`';
    }
    
    $query = 'SELECT ' . implode(',', $fields) . ' FROM `deletion_requests` AS r';
    $query .= ' LEFT JOIN `active_deletions` AS a ON a.`request_id` = r.`id`';
    if ($users !== NULL)
      $query .= ' INNER JOIN `users` AS u ON u.`id` = r.`user_id`';

    $where_clause = array();
    $params = array('');

    if ($users === NULL) {
      $where_clause[] = 'r.`user_id` = ?';
      $params[0] .= 'i';
      $params[] = &$this->_uid;
    }

    if ($request_id !== NULL) {
      $where_clause[] = 'r.`id` = ?';
      $params[0] .= 'i';
      $params[] = &$request_id;
    }
    else {
      // limit to live requests
      $where_clause[] = 'r.`status` IN (\'new\', \'activated\')';

      if ($users !== NULL && !in_array('*', $users)) {
        $quoted = array();
        foreach ($users as $u)
          $quoted[] = sprintf('\'%s\'', $u);
        $ulist = '(' . implode(',', $quoted) . ')';

        $where_clause[] = '(u.`name` IN ' . $ulist . ' OR u.`dn` IN ' . $ulist . ')';
      }

      if ($sites !== NULL || $items !== NULL) {
        $this->get_ids_by_sites_items('deletion', $sites, $items, $params, $where_clause);
        $where_clause[] = 'r.`id` IN (SELECT `id` FROM `deletion_ids_tmp`)';
      }
    }

    if (count($where_clause) != 0)
      $query .= ' WHERE ' . implode(' AND ', $where_clause);

    $query .= ' ORDER BY r.`id`';

    $stmt = $this->_db->prepare($query);

    if (count($params) > 1)
      call_user_func_array(array($stmt, "bind_param"), $params);

    if ($users !== NULL)
      $stmt->bind_result($rid, $status, $timestamp, $reason, $astatus, $atimestamp, $uname, $udn);
    else
      $stmt->bind_result($rid, $status, $timestamp, $reason, $astatus, $atimestamp);

    $stmt->execute();

    $_rid = 0;
    while ($stmt->fetch()) {
      if ($rid != $_rid) {
        $_rid = $rid;

        $data[$rid] = array(
          'request_id' => $rid,
          'item' => array(),
          'status' => $status,
          'requested' => $timestamp
        );

        $datum = &$data[$rid];

        if ($users !== NULL) {
          $datum['user'] = $uname;
          $datum['dn'] = $udn;
        }

        if ($status == 'rejected')
          $datum['reason'] = $reason;
        else if ($status == 'activated' || $status == 'completed')
          $datum['deletion'] = array();
      }

      if ($astatus !== NULL)
        $datum['deletion'][] = array('status' => $astatus, 'updated' => $atimestamp);
    }

    $stmt->close();

    // get the items for these requests
    $query = 'SELECT s.`request_id`, s.`site` FROM `deletion_request_sites` AS s WHERE s.`request_id` IN (';
    $query .= implode(',', array_keys($data));
    $query .= ')';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_result($rid, $site);
    $stmt->execute();

    while ($stmt->fetch())
      $data[$rid]['site'][] = $site;

    $stmt->close();

    // get the items for these requests
    $query = 'SELECT i.`request_id`, i.`item` FROM `deletion_request_items` AS i WHERE i.`request_id` IN (';
    $query .= implode(',', array_keys($data));
    $query .= ')';

    $stmt = $this->_db->prepare($query);
    $stmt->bind_result($rid, $item);
    $stmt->execute();

    while ($stmt->fetch())
      $data[$rid]['item'][] = $item;

    $stmt->close();

    return $data;
  }

  private function create_deletion_request($items, $sites)
  {
    // param
    //  items: array
    //  site: string

    $query = 'INSERT INTO `deletion_requests` (`user_id`, `timestamp`) VALUES (?, NOW())';
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('i', $site, $this->_uid);
    $stmt->execute();
    $request_id = $stmt->insert_id;
    $stmt->close();

    $query = sprintf('INSERT INTO `deletion_request_sites` (`request_id`, `site`) VALUES (%d, ?)', $request_id);
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $site_name);
    foreach ($sites as $nm) {
      $site_name = $nm;
      $stmt->execute();
    }
    $stmt->close();

    $query = sprintf('INSERT INTO `deletion_request_items` (`request_id`, `item`) VALUES (%d, ?)', $request_id);
    $stmt = $this->_db->prepare($query);
    $stmt->bind_param('s', $item_name);
    foreach ($items as $nm) {
      $item_name = $nm;
      $stmt->execute();
    }
    $stmt->close();

    return $request_id;
  }

  private function cancel_deletion_request($request_id)
  {
    $query = sprintf('UPDATE `deletion_requests` AS r SET `status` = \'cancelled\' WHERE r.`id` = %d', $request_id);
    $this->_db->query($query);

    $query = sprintf('DELETE FROM `active_deletions` WHERE `request_id` = %d', $request_id);
    $this->_db->query($query);
  }

  private function get_ids_by_sites_items($table_type, $sites, $items)
  {
    // Make a temporary table with ids of requests that contain the given sites and items

    // param
    //  table_type: 'copy' or 'deletion'
    //  sites: array or NULL
    //  items: array or NULL

    // write the lists to temporary tables first

    $query = sprintf('CREATE TEMPORARY TABLE `%s_sites_tmp` (', $table_type);
    $query .= '`site` varchar(32) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,';
    $query .= 'UNIQUE KEY `site` (`site`)';
    $query .= ')';
    $this->_db->query($query);

    if ($sites !== NULL) {
      $stmt = $this->_db->prepare(sprintf('INSERT INTO `%s_sites_tmp` VALUES (?)', $table_type));
      $stmt->bind_param('s', $site_name);
      foreach ($sites as $nm) {
        $site_name = $nm;
        $stmt->execute();
      }
      $stmt->close();
    }

    $query = sprintf('CREATE TEMPORARY TABLE `%s_items_tmp` (', $table_type);
    $query .= '`item` varchar(512) CHARACTER SET latin1 COLLATE latin1_general_cs NOT NULL,';
    $query .= 'UNIQUE KEY `item` (`item`)';
    $query .= ')';
    $this->_db->query($query);

    if ($items !== NULL) {
      $stmt = $this->_db->prepare(sprintf('INSERT INTO `%s_items_tmp` VALUES (?)', $table_type));
      $stmt->bind_param('s', $item_name);
      foreach ($items as $nm) {
        $item_name = $nm;
        $stmt->execute();
      }
      $stmt->close();
    }

    $query = sprintf('CREATE TEMPORARY TABLE `%s_ids_tmp` (', $table_type);
    $query .= '`id` INT(10) NOT NULL,';
    $query .= 'UNIQUE KEY `id` (`id`)';
    $query .= ')';
    $this->_db->query($query);

    $query = sprintf('INSERT INTO `%s_ids_tmp`', $table_type);
    $query .= sprintf(' SELECT r.`id` FROM `%s_requests` AS r WHERE', $table_type);
    $query .= sprintf(' 0 NOT IN (SELECT (`site` IN (SELECT `site` FROM `%s_request_sites` AS s WHERE s.`request_id` = r.`id`)) FROM `%s_sites_tmp`)', $table_type, $table_type);
    $query .= ' AND ';
    $query .= sprintf(' 0 NOT IN (SELECT (`item` IN (SELECT `item` FROM `%s_request_items` AS i WHERE i.`request_id` = r.`id`)) FROM `%s_items_tmp`)', $table_type, $table_type);
    
    $stmt = $this->_db->prepare($query);
    $stmt->execute();
    $stmt->close();
  }

  private function lock_table($operation = '')
  {
    if ($operation == 'copy')
      $query = 'LOCK TABLES `copy_requests` WRITE, `copy_requests` AS r WRITE, `active_copies` AS a WRITE, `copy_request_sites` WRITE, `copy_request_sites` AS s WRITE, `copy_request_items` WRITE, `copy_request_items` AS i WRITE';
    else if ($operation == 'deletion')
      $query = 'LOCK TABLES `deletion_requests` WRITE, `deletion_requests` AS r WRITE, `active_deletions` AS a WRITE, `deletion_request_sites` WRITE, `deletion_request_sites` AS s WRITE, `deletion_request_items` WRITE, `deletion_request_items` AS i WRITE';
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
