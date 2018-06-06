<?php

function single_rrd_to_array($rrd,$rrdpath){

  global $rrdcolumns;
  $ncols = count($rrdcolumns);
  $last = rrd_last($rrdpath . '/' . $rrd);


  $options = array('LAST', sprintf('--start=%d', $last - 3600 * 24 * 6), sprintf('--end=%d', $last - 1));
  $dump = rrd_fetch($rrdpath . '/' . $rrd, $options);//, count($options));                                                                                                                                 


  $there_entries = array();
  $enroute_entries = array();
  $missing_entries = array();
  $time_entries = array();

  foreach ( $dump["data"]["there"] as $key => $value )
  {
    array_push($time_entries,$key);
    array_push($there_entries,$value);
  }
  foreach ( $dump["data"]["missing"] as $key => $value )
  {
    array_push($missing_entries,$value); 
  }
  foreach ( $dump["data"]["enroute"] as $key => $value )
  {
    array_push($enroute_entries,$value);
  }
  $last_enroute = array_pop($enroute_entries); 
  $last_there = array_pop($there_entries);
  $last_missing = array_pop($missing_entries);
  $last_time = array_pop($time_entries);

  while(count($missing_entries)!=0 and is_nan($missing_entries[0])){
    $enroute_missing = array_shift($enroute_entries);
    $first_missing = array_shift($missing_entries);
    $first_there = array_shift($there_entries);
    $first_time = array_shift($time_entries);
  }

  $size = count($missing_entries);

  $rrd_array = array(
                     $time_entries,
                     $missing_entries,
                     $enroute_entries,
                     $there_entries,
                     );

  return $rrd_array;

}

date_default_timezone_set('America/New_York');

// Preparing the html string to be replaced in the static enforcer.html

// Fetching first ruleset as default for dropdown
$counter = 0;
$var = "";
foreach (glob(__DIR__ . '/monitoring_enforcer/*') as $filename) {
  if ($counter > 0){
    break;
  }
  $var = basename($filename, ".rrd");
  $counter += 1;
}

// Now preparing dropdown
$html_replace = '<form method="GET">';
$html_replace = $html_replace .'<select name="rule" onchange="this.form.submit()">';
$html_replace = $html_replace . "<option value='Choose policy'>" . "Choose policy" . "</option>";

foreach (glob(__DIR__ . '/monitoring_enforcer/*.rrd') as $filename) {
  if ($var === $filename){
    $html_replace = $html_replace . "<option selected='selected'>" . basename(str_replace(".rrd","",$filename)) . "</option>";
  }
  $html_replace = $html_replace . "<option value=" . basename(str_replace(".rrd","",$filename)) .">" . basename(str_replace(".rrd","",$filename)) . "</option>";
}

$html_replace = $html_replace .'</select>';
$html_replace = $html_replace .'</form>';


if (isset($_GET['rule'])){
  $var=$_GET['rule'];
}

$jsondata = file_get_contents("https://raw.githubusercontent.com/SmartDataProjects/dynamo-policies/master/common/enforcer_rules_physics.json");
$lines = explode("\n", $jsondata);

$array = json_decode($jsondata,true);

$considered_sites = array();
$considered_backends = array();
$considered_lat = array();
$considered_long = array();

function get_sites($rulename) {

  global $array;
  global $considered_sites;
  global $considered_backends;
  global $considered_lat;
  global $considered_long;
  $considered_sites = array();
  $considered_backends = array();
  $considered_lat = array();
  $considered_long = array();

  foreach($array['rules'] as $key => $value) {
    if ($key != $rulename){
      continue;
    } 



    foreach($value['destinations'] as $pattern => $val) {
      $site_patterns = array();

      if (strpos($val, 'site.name == ') !== false) {
        $site_patterns[] = str_replace('site.name == ','',$val);
      }
      else {
        $site_patterns=explode(' ',str_replace(']','',str_replace('[','',strstr($val,'['))));
      }  
    
      foreach($site_patterns as &$value){ 
        if ($handle = fopen(__DIR__ . "/geodata/lat_long_sites.csv", "r")) {
          $count = 0;
          while (($line = fgets($handle, 4096)) !== FALSE) {
	    $line_array = explode(",",$line);
	    if (fnmatch($value,$line_array[0])){ 
	      $considered_sites[] = $line_array[0];
	      $considered_backends[] = $line_array[1];
	      $considered_lat[] = $line_array[2];
	      $considered_long[] = $line_array[3];
	    }
	    $count++;
          }
          fclose($handle);
        }
      }
    }
  }
}

get_sites($var);


//echo $considered_sites[0];
foreach($considered_sites as &$value){
//echo $value;
}

// Obtain status from rrd file
if ( (isset($_REQUEST['getStatus']) && $_REQUEST['getStatus'])){

  $status_array = array();
  $directory = 'monitoring_enforcer/';

  if (! is_dir($directory)) {
    exit('Invalid diretory path');
  }

  foreach (scandir($directory) as $file) {
    if ('.' === $file) continue;
    if ('..' === $file) continue;
    if (isset($_REQUEST['whichRule'])) {
      if (str_replace(".rrd","",$file) != $_REQUEST['whichRule']){
	continue;
      }
    }
    else if ($var != $file) continue;

    if (($handle = fopen("monitoring_enforcer/" . $file, "r")) !== FALSE) {

      $search      = str_replace('.rrd','',$file);
      $line_number = false;
     
      get_sites($search);
 
      $count = 0;
      foreach($lines as $word) {
	if (!$line_number){
	  $count++;
	  $line_number = (strpos($word, $search) !== FALSE) ? $count : $line_number;
	}
      }

      $siteinfo = array('rule' => str_replace('.rrd','',$file)."_".$line_number, 'data' => array());
            
      $rrd_info = single_rrd_to_array($file,__DIR__ . "/monitoring_enforcer/");
      $siteinfo['data'][] = array('date' => $rrd_info[0], 'missing' => $rrd_info[1], 'enroute' => $rrd_info[2], 'there' => $rrd_info[3], 'sites' => $considered_sites, 'backends' => $considered_backends, 'lat' => $considered_lat, 'long' => $considered_long);
      $status_array[] = $siteinfo;
	
    }
  }

  echo @json_encode($status_array);
  
}

// Communciation with html file
if ( !(isset($_REQUEST['getStatus'])) ){

  $rule_replace = $var;
  $html = file_get_contents(__DIR__ . '/enforcer.html');
  $html = str_replace('${DROPDOWN}', "$html_replace", $html);
  $html = str_replace('${RULE}', "$rule_replace", $html);
  echo $html;

}

?>
