<?php

$site = $_REQUEST['site'];
$site = str_replace('"', "", $site);
#site = "T2_US_Florida";

$localpath = '/var/www/html';
$pngpath = $localpath . '/dynamo/dealermon';

$html = '<html>' . "\n";
$html .= '  <head>' . "\n";
$html .= '    <title>Transfer Monitoring</title>' . "\n";
$html .= '    <style>' . "\n";
$html .= 'body {' . "\n";
$html .= '  font-family:helvetica;' . "\n";
$html .= '}' . "\n";
$html .= 'table {' . "\n";
$html .= '  border:1px solid black;' . "\n";
$html .= '  border-collapse:collapse;' . "\n";
$html .= '}' . "\n";
$html .= 'tr {' . "\n";
$html .= '  border:1px solid black;' . "\n";
$html .= '}' . "\n";
$html .= 'th {' . "\n";
$html .= '  background-color:#cccccc;' . "\n";
$html .= '  border:1px solid black;' . "\n";
$html .= '}' . "\n";
$html .= 'td {' . "\n";
$html .= '  border:1px solid black;' . "\n";
$html .= '}' . "\n";
$html .= 'tr.odd {' . "\n";
$html .= '  background-color:#eeeeee;' . "\n";
$html .= '}' . "\n";
$html .= 'tr.even {' . "\n";
$html .= '  background-color:#ffffff;' . "\n";
$html .= '}' . "\n";
$html .= 'td.data {' . "\n";
$html .= '  text-align:right;' . "\n";
$html .= '}' . "\n";
$html .= 'div.graphs {' . "\n";
$html .= '  width:810px;' . "\n";
$html .= '  margin:10px 0 10px 0;' . "\n";
$html .= '}' . "\n";
$html .= 'div.username {' . "\n";
$html .= '  font-size:150%;' . "\n";
$html .= '  font-weight:bold;' . "\n";
$html .= '  text-align:left;' . "\n";
$html .= '  margin-bottom:10px;' . "\n";
$html .= '}' . "\n";
$html .= '    </style>' . "\n";
$html .= '    <meta http-equiv="refresh" content="300">' . "\n";
$html .= '  </head>' . "\n";
$html .= '  <body>' . "\n";

$images = '';

echo "Overview for " . $site . "<br>";

foreach (glob($pngpath . "/monitoring/request__$site*.png") as $key=>$filename) {
  
#  echo $key;

  $filename = str_replace($localpath,'',$filename);

  if ($key == 0)
    $images .= '    <b>Requests</b>' . "\n"; 
  
  $images .= '    <div class="graphs">' . "\n";
  $images .= '      <img src="' . $filename . '">' . "\n";
  $images .= '    </div>' . "\n";

}
#$html .= '  <b>This text is bold</b>' . "\n";
foreach (glob($pngpath . "/monitoring/replica__$site*.png") as $keys=>$filename) {

  $filename = str_replace($localpath,'',$filename);  

  if ($keys == 0)
    $images .= '    <b>Replicas</b>' . "\n"; 
  
  $images .= '    <div class="graphs">' . "\n";
  $images .= '      <img src="' . $filename . '">' . "\n";
  $images .= '    </div>' . "\n";

}

$html .= $images;
$html .= '  </body>' . "\n";
$html .= '</html>' . "\n";

echo $html;
?>