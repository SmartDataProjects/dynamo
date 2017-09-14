<?php
  //Returns data for use in the auxilliary site (phedex_url) 
function escapeJavaScriptText($string) 
{ 
  return str_replace("\n", '\n', str_replace('"', '\"', addcslashes(str_replace("\r", '', (string)$string), "\0..\37'\\"))); 
} 

$site = "T2_US_MIT";

if  ((isset($_REQUEST['site']))){
  $site = $_REQUEST['site'];
  //$site = escapeJavaScriptText($site);
}


$serviceId = 1;
$csvpath = array();
$rrdpaths = array();

if ((isset($_REQUEST['serviceId']) && $_REQUEST['serviceId'] == 1)){

  $csvpath[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring/" . $site . "/filelist.txt";
  $rrdpaths[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring/";

}

if ((isset($_REQUEST['serviceId']) && $_REQUEST['serviceId'] == 2)){

  $csvpath[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring_phedex/". $site . "/filelist.txt";
  $rrdpaths[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring_phedex/"; 
  $serviceId = 2;
}

if ((isset($_REQUEST['serviceId']) && $_REQUEST['serviceId'] == 3)){
  $csvpath[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring/" . $site . "/filelist.txt";
  $csvpath[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring_phedex/" . $site . "/filelist.txt";
  $rrdpaths[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring/";
  $rrdpaths[] = "/var/www/html/dynamo/dynamo/dealermon/monitoring_phedex/"; 
  $serviceId = 3;
}


function single_site_csvline_to_array($csvdata, $DorP){
  
  $filename  = $csvdata[1];
  $id = $csvdata[0];
  $total = $csvdata[3];
  $copied = $csvdata[2];
  $isstuck = $csvdata[4];
  
  $filename1 = substr($filename, 1);
  $filename2 = str_replace("/", "+", $filename1);
  
  $filename3 = $DorP . $filename2;

  $csvline = array(
		   $filename3,
		   $id,
		   intval($total),
		   intval($copied),
		   intval($isstuck)
		   );
  return $csvline;
}


if ( (isset($_REQUEST['getSiteCSVs']) && $_REQUEST['getSiteCSVs']) ){
  $final = array();
  $row = 1;
  for($i =0; $i < count($csvpath); $i++){

    if (($handle = fopen($csvpath[$i], "r")) !== FALSE){ 
      $DorP = "D";
      if( strpos($csvpath[$i], 'phedex') !== false){
	  $DorP = "P";

	}	 
      $counter = 0;
      while (($data = fgetcsv($handle, 10000, ",")) !== FALSE){
	if ($counter++ == 0)
	  continue;
	
	$row++;
	$line = single_site_csvline_to_array($data,$DorP);
	
	$final[] = $line;
      }
    }
    
  }

  echo @json_encode($final);
}


$rrdcolumns = array('copied', 'total');

function single_rrd_to_array($rrd,$rrdpath){

  global $rrdcolumns;
  $ncols = count($rrdcolumns);
  $last = rrd_last($rrdpath . '/' . $rrd);

  $options = array('LAST', sprintf('--start=%d', $last - 3600 * 24 * 6), sprintf('\
--end=%d', $last - 1));
  $dump = rrd_fetch($rrdpath . '/' . $rrd, $options, count($options));

  if (isset($dump['data']) && count($dump['data']) >= $ncols) {
    $chunks = array_chunk($dump['data'], $ncols);
    $entry = $chunks;
  }
  else
    $entry = array_fill(0, $ncols, 0);

  $copied_entries = array();
  $total_entries = array();
  $time_entries = array();

  $mapped = array();
  $counter = 0;

  foreach ($entry as $i => $d){
    if ($counter%1 == 0) {
      array_push($time_entries,$i*$dump["step"]+$dump["start"]);      array_push($total_entries,$d[1]/1e12);
      array_push($copied_entries,$d[0]/1e12);
    }
    $counter = $counter + 1;
  }

  $last_copied = array_pop($copied_entries);
  $last_total = array_pop($total_entries);
  $last_time = array_pop($time_entries);

  while(count($total_entries)!=0 and is_nan($total_entries[0])){
    $first_total = array_shift($total_entries);
    $first_copied = array_shift($copied_entries);
    $first_time = array_shift($time_entries);
  }

  $size = count($total_entries);
  $rrd_array = array(
                     $time_entries,
                     $copied_entries,
		     $total_entries,
		     );

  return $rrd_array;
}

if ( (isset($_REQUEST['getSiteRRDs']) && $_REQUEST['getSiteRRDs']) ){


  $final = array();
  $d = array();
  for($i = 0; $i < count($rrdpaths); $i++){  

    foreach (glob($rrdpaths[$i] . "*") as $sitename){

      $site1 = str_replace($rrdpaths[$i], '', $sitename);
      if ($site1 !=  $_REQUEST['site'])
	continue;
      $siteinfo = array('site' => $site1, 'data' => array());
 
      foreach (glob($sitename . "/*.rrd") as $replicaname) {


	$replicaname = str_replace($sitename . '/', '', $replicaname);
	$replicaname1 = str_replace('.rrd', '', $replicaname);
	$replicaname_explode = explode("_", $replicaname1, 2);
	$replicaname2 = $replicaname_explode[1];

	if ($replicaname2 !=  str_replace(' ', "+",$_REQUEST['replicaname']))

	  continue;
	$replicadata = single_rrd_to_array($replicaname, $sitename);
	
	$ratio = array();

	for($j = 0; $j < count($replicadata[2]); $j++){
	  $ratio[] = $replicadata[1][$j]/$replicadata[2][$j];
	}
	$replicainfo = array('replica' => $replicaname1, 'time' => $replicadata[0], 'copied' => $replicadata[1], 'total' => $replicadata[2], 'ratio' => $ratio);
	$siteinfo['data'][] = $replicainfo;
      }
      $d[] = $siteinfo;
    }
  }

  echo @json_encode($d);
}


if ( !(isset($_REQUEST['getSiteCSVs'])) ){

  
  $html = file_get_contents(__DIR__ . '/csvs.html');
  $html = str_replace('${SITENAME}', "$site", $html);
  $html = str_replace('${SERVICE}', "$serviceId", $html);

  echo $html;
  
}

?>

