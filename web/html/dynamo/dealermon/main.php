<?php

$pngpath = '/var/www/html/dynamo/dealermon';

$rrds = array();
$pngs = array();
$sites = array();

$dirp = opendir($pngpath);

while (($ent = readdir($dirp)) !== false) {
  if ($ent == "." || $ent == "..")
    continue;

  if (strpos($ent, ".png") == strlen($ent) - 4)
    $pngs[] = $ent;
}

#echo $pngs;
closedir($dirp);

#$s = 'Posted On April 6th By Some Dude';
#echo strstr($s, 'By', true);


#print $pngs

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
$html_format = $html;

$images = '';

$images .= '    <b>DYNAMO DEALER MONITORING</b>' . "\n";

$images .= '    <div class="graphs">' . "\n";
$images .= '      <div class="site"><a">Total</a></div>' . "\n";
$images .= '      <img src="monitoring/total.png">' . "\n";
$images .= '    </div>' . "\n";


foreach (glob($pngpath . "/monitoring/site*.png") as $filename) {
  #echo "$filename "  . "\n";
  $site = str_replace('.png', '', $filename);
  #echo "$site "  . "\n";
  $site = str_replace('/var/www/html/dynamo/dealermon/monitoring/site__', '', $site);
  $sites[] = $site;
  $filename = str_replace('/var/www/html/dynamo/dealermon/','',$filename);


  $images .= '    <div class="graphs">' . "\n";
  $images .= '      <div class="site"><a href=site.php?site="' . $site . '">' . $site . '</a></div>' . "\n";
  $images .= '      <img src="' . $filename . '">' . "\n";
  $images .= '    </div>' . "\n";
  
}

$html .= $images;
$html .= '  </body>' . "\n";
$html .= '</html>' . "\n";

echo $html;
?>