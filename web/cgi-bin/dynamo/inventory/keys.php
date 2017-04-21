<?php

// generator of return data keys - corresponds to values of $categories and can be executed as
// $categories($keys) where $keys is an array of [dataset_name, site_name, group_name]

function campaigns($name)
{
  preg_match('/^\/[^\/]+\/(?:(Run20.+)-v[0-9]+|([^\/-]+)-[^\/]+)\/.*/', $name, $matches);
  if (count($matches) > 2 && $matches[2] != "")
    return $matches[2];
  else
    return $matches[1];
}

function dataTiers($name)
{
  preg_match('/^\/[^\/]+\/[^\/]+\/([^\/-]+)/', $name, $matches);
  return $matches[1];
}

function datasets($name)
{
  return $name;
}

function sites($name)
{
  return $name;
}

function groups($name)
{
  return $name;
}

?>