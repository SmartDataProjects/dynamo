<?php

function array_quote($arr)
{
  $res = array();
  foreach ($arr as $elem)
    $res[] = sprintf('\'%s\'', $elem);

  return $res;
}

?>