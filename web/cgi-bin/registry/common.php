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

  $query = 'SELECT u.`id`, u.`name`, r.`id` FROM `dynamoserver`.`users` AS u, `dynamoserver`.`roles` AS r WHERE (u.`dn` = ? OR u.`dn` = ?) AND r.`name` = ?';
  $stmt = $db->prepare($query);
  $stmt->bind_param('sss', $cert_dn, $issuer_dn, $service);
  $stmt->bind_result($uid, $uname, $sid);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($uid == 0 || $sid == 0)
    return false;

  // check if this user has admin permission
  $query = 'SELECT COUNT(*) FROM `dynamoserver`.`user_authorizations` AS a';
  $query .= ' INNER JOIN `dynamoserver`.`roles` AS r ON r.`id` = a.`role_id`';
  $query .= ' WHERE a.`user_id` = ? AND r.`name` = "admin" AND a.`target` = "registry"';

  $stmt = $db->prepare($query);
  $stmt->bind_param('i', $uid);
  $stmt->bind_result($count);
  $stmt->execute();
  $stmt->fetch();
  $stmt->close();

  if ($count == 0) {
    // a normal user - (user, service) must be in authorized_users table

    $query = 'SELECT COUNT(*) FROM `dynamoserver`.`user_authorizations` WHERE `user_id` = ? AND `role_id` = ? AND `target` = "registry"';

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

    $query = 'SELECT a.`user_id` FROM `dynamoserver`.`user_authorizations` AS a';
    $query .= ' INNER JOIN `dynamoserver`.`users` AS u ON u.`id` = a.`user_id`';
    $query .= ' WHERE a.`role_id` = ? AND u.`name` = ? AND a.`target` = "registry"';

    $stmt = $db->prepare($query);
    $stmt->bind_param('is', $sid, $as_user);
    $stmt->bind_result($uid);
    $stmt->execute();
    $result = $stmt->fetch();
    $stmt->close();

    return $result;
  }
}

function jsonize($array)
{
  // serially convert PHP data structure to JSON without integrity check.

  if (count($array) == 0 || array_keys($array) === range(0, count($array) - 1)) {
    // sequential array
    $json = '[';
    
    $delim = '';
    foreach ($array as $elem) {
      $json .= $delim;

      if (is_array($elem))
        $json .= jsonize($elem);
      else if (is_numeric($elem))
        $json .= $elem;
      else if ($elem === NULL)
        $json .= 'null';
      else
        $json .= sprintf('"%s"', $elem);

      $delim = ', ';
    }

    $json .= ']';
  }
  else {
    // associative array
    $json = '{';

    $delim = '';
    foreach ($array as $key => $value) {
      $json .= sprintf('%s"%s": ', $delim, $key);

      if (is_array($value))
        $json .= jsonize($value);
      else if (is_numeric($value))
        $json .= $value;
      else if ($value === NULL)
        $json .= 'null';
      else
        $json .= sprintf('"%s"', $value);

      $delim = ', ';
    }

    $json .= '}';
  }

  return $json;
}

function send_response($code, $result, $message, $data = NULL, $format = 'json')
{
  header($_SERVER['SERVER_PROTOCOL'] . ' ' . $code, true, $code);

  if ($format == 'json') {
    $json = '{"result": "' . $result . '", "message": "' . $message . '"';

    if ($data !== NULL) {
      $json .= ', "data": ' . jsonize($data);
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
