<?php

function get_user($db, $cert_dn, $issuer_dn, $service, $as_user, &$uid, &$uname, &$sid)
{
  // fill in uid and sid, return true => authorized, false => unauthorized (still may be allowed to do read-only operations)

  // Apache mod_ssl version in CentOS returns a comma-delimited (and inverted) list of DN parts
  if (strpos($cert_dn, '/') !== 0) {
    // hopefully there aren't weird things like escaped commas..
    $dn_arr = explode(',', $cert_dn);
    $cert_dn = "";
    foreach (array_reverse($dn_arr) as $part)
      $cert_dn .= '/' . $part;
  }
  if (strpos($issuer_dn, '/') !== 0) {
    // hopefully there aren't weird things like escaped commas..
    $dn_arr = explode(',', $issuer_dn);
    $issuer_dn = "";
    foreach (array_reverse($dn_arr) as $part)
      $issuer_dn .= '/' . $part;
  }

  // get the user id
  $uid = 0;
  $sid = 0;

  $query = 'SELECT `users`.`id`, `users`.`name`, `services`.`id` FROM `users`, `services` WHERE (`users`.`dn` = ? OR `users`.`dn` = ?) AND `services`.`name` = ?';
  $stmt = $db->prepare($query);
  $stmt->bind_param('sss', $cert_dn, $issuer_dn, $service);
  $stmt->bind_result($uid, $uname, $sid);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($uid == 0 || $sid == 0)
    return false;

  // check if this user has admin permission
  $query = 'SELECT COUNT(*) FROM `authorized_users`';
  $query .= ' INNER JOIN `services` ON `services`.`id` = `authorized_users`.`service_id`';
  $query .= ' WHERE `authorized_users`.`user_id` = ? AND `services`.`name` = "admin"';

  $stmt = $db->prepare($query);
  $stmt->bind_param('i', $uid);
  $stmt->bind_result($count);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($count == 0) {
    // a normal user - (user, service) must be in authorized_users table

    $query = 'SELECT COUNT(*) FROM `authorized_users` WHERE `user_id` = ? AND `service_id` = ?';

    $stmt = $db->prepare($query);
    $stmt->bind_param('ii', $uid, $sid);
    $stmt->bind_result($count);
    $stmt->execute();
    $stmt->fetch();
    $stmt->close();

    return $count > 0;
  }
  else if ($as_user === NULL) {
    // admin as herself - any service is allowed

    return true;
  }
  else {
    // admin as someone else - (as_user, service) must be in authorized_users table

    $uname = $as_user;

    $query = 'SELECT `authorized_users`.`user_id` FROM `authorized_users`';
    $query .= ' INNER JOIN `users` ON `users`.`id` = `authorized_users`.`user_id`';
    $query .= ' WHERE `authorized_users`.`service_id` = ? AND `users`.`name` = ?';

    $stmt = $db->prepare($query);
    $stmt->bind_param('is', $sid, $as_user);
    $stmt->bind_result($uid);
    $stmt->execute();
    $result = $stmt->fetch();
    $stmt->close();

    return $result;
  }
}

function send_response($code, $result, $message, $data = NULL, $format = 'json')
{
  header($_SERVER['SERVER_PROTOCOL'] . ' ' . $code, true, $code);

  if ($format == 'json') {
    $json = '{"result": "' . $result . '", "message": "' . $message . '"';

    if ($data !== NULL) {
      $json .= ', "data": [';
      $data_json = array();
      foreach ($data as $elem) {
        $j = array();
        foreach($elem as $key => $value) {
          $kv = '"' . $key .'": ';
          if (is_string($value))
            $kv .= '"' . $value . '"';
          else
            $kv .= '' . $value;

          $j[] = $kv;
        }
        $data_json[] = '{' . implode(', ', $j) . '}';
      }
      $json .= implode(', ', $data_json);
      $json .= ']';
    }
  
    echo $json . "}\n";
  }
  else {
    $writer = new XMLWriter();
    $writer->openMemory();
    $writer->setIndent(true);

    $writer->startDocument('1.0', 'UTF-8');

    $writer->startElement('data');

    $writer->startElement('result');
    $writer->text($result);
    $writer->endElement();

    $writer->startElement('message');
    $writer->text($message);
    $writer->endElement();

    if ($data !== NULL) {
      $writer->startElement('locks');
      foreach ($data as $elem) {
        $writer->startElement('lock');
        $writer->startAttribute('id');
        $writer->text($elem['lockid']);
        $writer->endAttribute();
        foreach($elem as $key => $value) {
          if ($key == 'lockid')
            continue;

          $writer->startElement($key);
          $writer->text($value);
          $writer->endElement();
        }
        $writer->endElement();
      }
      $writer->endElement();
    }

    $writer->endElement();

    $writer->endDocument();

    echo $writer->flush();
  }
 
  exit(0);
}

?>
