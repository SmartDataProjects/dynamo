<?php

include_once('/var/www/cgi-bin/dynamo/common/db_conf.php');

//Function to get an array of total aggregate requested data size ordered by their calendar date of request
if ( (isset($_REQUEST['getHistory']) && $_REQUEST['getHistory']) ) {
  $history_db = new mysqli($db_conf['host'], $db_conf['user'], $db_conf['password'], 'dynamohistory');
  $aggregate_array = array(); 
  $result = $history_db->query("SELECT UNIX_TIMESTAMP(`timestamp`) as `unix`, SUM(`size`) as `sum` FROM `copy_requests` WHERE DATE(`timestamp`) > DATE_SUB(CURDATE(), INTERVAL 50 DAY) GROUP BY DATE(`timestamp`) ORDER BY `copy_requests`.`timestamp` DESC "); //query request from database-- selecting only the timestamp and sum of size-- grouped together for sum by the calendar date of the unix `timestamp`

  if ($result->num_rows > 0)  {
    while($row = $result->fetch_assoc()) {
      $tmp_array = array('date' => $row["unix"], 'size' => $row["sum"]);
      $aggregate_array[] = $tmp_array;//appends these values into an empty array
    }  
    echo @json_encode($aggregate_array);
  } else {
    echo "no results";//executes if database is empty or no requests made within past 5 days
  }
}


// Which service(s) are we looking at?
if ((isset($_REQUEST['getServices']) && $_REQUEST['getServices']) ) {

  $data = array();

  $type_id = 1;
  $type_name = 'Dynamo';
  $elem = array('id' => $type_id, 'name' => $type_name);
  $data[] = $elem;
  $type_id = 2;
  $type_name = 'Other';
  $elem = array('id' => $type_id, 'name' => $type_name);
  $data[] = $elem;
  $type_id = 3;
  $type_name = 'Compare';
  $elem = array('id' => $type_id, 'name' => $type_name);
  $data[] = $elem;

  echo json_encode($data);
}


// Dynamo is default
$service_id = 1;
$rrdpaths = array('./monitoring/');
$overviewpaths = array('monitoring/overview.txt');
$summarypaths = array('./monitoring');

if ((isset($_REQUEST['serviceId']) && $_REQUEST['serviceId'] == 2)){
  $service_id = 2;
  $rrdpaths = array('./monitoring_phedex/');
  $overviewpaths = array("monitoring_phedex/overview.txt");
  $summarypaths[0] = './monitoring_phedex';
}

if ((isset($_REQUEST['serviceId']) && $_REQUEST['serviceId'] == 3)){
  $service_id = 3;
  $rrdpaths = array('./monitoring_phedex/');
  $overviewpaths = array('monitoring_phedex/overview.txt','monitoring/overview.txt');
  $summarypaths[] = './monitoring_phedex';
}


$rrdcolumns = array('copied', 'total');

// function to read rrd file and return array (timestamp, copied, total)
function single_rrd_to_array($rrd,$rrdpath){

  global $rrdcolumns;
  $ncols = count($rrdcolumns);
  $last = rrd_last($rrdpath . '/' . $rrd);

  $options = array('LAST', sprintf('--start=%d', $last - 3600 * 24 * 6), sprintf('--end=%d', $last - 1));
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
      array_push($time_entries,$i*$dump['step']+$dump['start']);
      array_push($total_entries,$d[1]/1e12);
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


// Reading rrd with total volume of all ongoing requests
if (isset($_REQUEST['getSummary']) && $_REQUEST['getSummary']) {
  $Summary = array();
  
  for($i = 0; $i < count($summarypaths); $i++){  
      $Summary[] = single_rrd_to_array('total.rrd',$summarypaths[$i]);
    }
	
  echo json_encode($Summary);

}


// Function to read files with site overviews
function get_site_overview_from_csv($file){

  $sites = array();
  $total_total = array();
  $total_copied = array();
  $stuck_total = array();
  $stuck_copied = array();
  $nreplicas = array();

  for($i = 0; $i != count($file); $i++){    
    $row = 1;
    if (($handle = fopen($file[$i], "r")) !== FALSE) {
      $counter = 0;
      while (($data = fgetcsv($handle, 1000, ',')) !== FALSE) {
	if($counter ==0){
	  $counter+=1;
	  continue;
	}
	$row++;
	$site = $data[0];
	
	if (!in_array($site,$sites)){
	  array_push($sites,$site);
	}

	$nreplicas[$site] += intval($data[1]);
	$total_total[$site] += intval($data[2]);
	$total_copied[$site] += intval($data[3]);
	$stuck_total[$site] += intval($data[4]);
	$stuck_copied[$site] += intval($data[5]);
      }  
    }
  }

  foreach ($sites as $key => $site){
    $csv_array[] = array($site, $nreplicas[$site], $total_total[$site], $total_copied[$site], $stuck_total[$site], $stuck_copied[$site]);
    continue;
  }

  return $csv_array;

}


// Getting site overviews
if ( (isset($_REQUEST['getJson']) && $_REQUEST['getJson']) || (isset($_REQUEST['getSiteOverview']) && $_REQUEST['getSiteOverview'])) {

  $d = array();
  $d = get_site_overview_from_csv($overviewpaths);
  
  echo @json_encode($d);

}


// Communciation with html file
if ( !(isset($_REQUEST['getSummary'])) and !(isset($_REQUEST['getSiteCSVs'])) and !(isset($_REQUEST['getJson'])) and  !(isset($_REQUEST['getHistory'])) and  !(isset($_REQUEST['getCSVs'])) and !(isset($_REQUEST['getServices'])) and !(isset($_REQUEST['getSiteOverview']))){

  $html = file_get_contents(__DIR__ . '/dealermon.html');
  $html = str_replace('${SERVICE}', "$service_id", $html);

  echo $html;

}
?>

