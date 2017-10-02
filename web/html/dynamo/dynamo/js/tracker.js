// globally defined variables
var max_time = 0;
var one_day = 24*60*60;
var json = [];
var daily_requested_date = [];
var daily_requested_size = [];


function toggle(i,j){
    //Creates the toggle buttons for Compare Tab:

    
    if (i == 1 || i == 4 || i == 6){
      var sid = (i == 1)*1 + (i != 1)*i/2;
	var ajaxInput_ongoings = {
	    'url': 'main.php',
	    'data': {'getJson': 1, 'serviceId': sid},
	    'success': function (data) { 
		
		drawSiteOverview(data,sid);
	    },
	    'dataType': 'json',
	    'async': false
	};
	
	$.ajax(ajaxInput_ongoings);
   
	var ajaxInput_summary = {
	    'url': 'main.php',
	    'data': {'getSummary': 1, 'serviceId' : sid},
	    'success': function (data) { 
		drawSummary(data,sid);
	    },
	    'dataType': 'json',
	    'async': false
	};
	
	$.ajax(ajaxInput_summary);

    }     

}


// Basic function to draw simple traces
function makeTrace(title,data_x,data_y,dash,widths,legend,colors){
    return{
	name: title,
	    x: data_x,
	    y: data_y,
	    line: {shape:'linear',
		color: colors,
		width: widths,
		dash: dash
		},
	    showlegend: legend,    
	    connectgaps: true

    }
}

// Function to convert UNIX timestamp into human readable format YYYY-MM-DD H:MIN:SEC
function timeConverter(UNIX_timestamp){
    var a = new Date(UNIX_timestamp * 1000);
    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    var year = a.getFullYear();
    var month = months[a.getMonth()];
    var date = a.getDate();
    var hour = a.getHours();
    var min = a.getMinutes();
    var sec = a.getSeconds();
    var time = year + '-' + month + '-' + date + ' ' + hour + ':' + min + ':' + sec +'0.000000';
    return time;
}

// Function to draw stacked traces
function stackedArea(traces) {
    for(var i=1; i<traces.length; i++) {
	for(var j=0; j<(Math.min(traces[i]['y'].length, traces[i-1]['y'].length)); j++) {
	    traces[i]['y'][j] += traces[i-1]['y'][j];
	}
    }
    return traces;
}

// Function to get latest time from json data - sets globally_defined max_time variable
function getLatestTime(data){
    for(var k = 0 in data){
	for(var i = 0 in data[k]) {
	    var obj = data[k][i];
	    for(var j in obj["data"]) {
		var replica = obj["data"][j];
		if (replica.time[replica.total.length-1]>max_time)
		    max_time = replica.time[replica.total.length-1];
	    }
	}
    }
}

// Function for pushing the daily requested sizes from JSON into array for better handling
function getDailyRequested(data) {

    for (i = 0; i<data.length;i++){

        var obj = data[i];
	daily_requested_date.push(obj.date);

	daily_requested_size.push(obj.size);
       
    }
 
}

// Initializing page
function initPage(serviceId) {

    // List of ajax calls to get data from either the database directly (history) or from rrd files (ongoings, summary) 
    
    if (serviceId != 3){
	document.getElementById("togglePhedex").style.display = "none";
	document.getElementById("toggleDynamo").style.display = "none";
	document.getElementById("toggleSum").style.display = "none";
    }
    
    var ajaxInput_services = {

        'url': 'http://dynamo.mit.edu/dynamo/dealermon/main.php',
        'data': {'getServices': 1},
        'success': function (data) { 
	    setServices(data);
	},
        'dataType': 'json',
        'async': false
    };

    $.ajax(ajaxInput_services);

    var ajaxInput_ongoings = {
        'url': 'http://dynamo.mit.edu/dynamo/dealermon/main.php',
        'data': {'getJson': 1, 'serviceId': serviceId},
        'success': function (data) { 
	    drawSiteOverview(data,serviceId);
	},
        'dataType': 'json',
        'async': false
    };

    $.ajax(ajaxInput_ongoings);

    var ajaxInput_summary = {
        'url': 'http://dynamo.mit.edu/dynamo/dealermon/main.php',
        'data': {'getSummary': 1, 'serviceId' : serviceId},
        'success': function (data) { 
 
	    drawSummary(data,serviceId);
	},
        'dataType': 'json',
        'async': false
    };

    $.ajax(ajaxInput_summary);

    d3.selectAll('.serviceTab')
	.classed('selected', false);
    d3.select('#service' + serviceId)
	.classed('selected', true);
    
}

//Creates bar graph data at bottom of Summary Graph:
function drawAggregates(total_time, total_copied, total_total){
   
    var midnights_time_tmp = [];
    var midnights_time = [];
    var midnights_copied = [];
    
    for (var idx = 0; idx<total_time.length; idx++){
	if ((total_time[idx]-86400) % one_day == 0){
	    midnights_time_tmp.push(total_time[idx]);
	}
    }

    for (var idx = midnights_time_tmp.length-1; idx>=0; idx--){
	midnights_time[midnights_time_tmp.length-1-idx]=midnights_time_tmp[idx];
    }
    
    //massaging times that get screwed up because of timezone difference:    
    midnights_time.reverse();
    midnights_time.unshift(midnights_time[0]-24*60*60);
    
    for (var i=0;i<midnights_time.length;i++){
    	midnights_time[i]+=18000;
    }

    var total_copied_aggr = [];
    
    for (var k=0;k<total_time.length;k++){
	if (k==0){
	    total_copied_aggr.push(total_copied[0]);
	}
	else {
	    total_copied_aggr.push(total_copied_aggr[k-1]+(total_copied[k]-total_copied[k-1])*(total_copied[k]>total_copied[k-1]));
	}
    }
    
    for (var m=0; m<midnights_time.length;m++){
	var idx_1 = total_time.indexOf(midnights_time[m]);
	var idx_2 = -99;
	if (m==midnights_time.length-2){
	    var nan_index = total_copied_aggr.length-1;
	    while (isNaN(total_copied_aggr[nan_index])){
		nan_index -= 1;
	    }
	    idx_2 = nan_index;
	}
	else{
	    idx_2 = total_time.indexOf(midnights_time[m+1]);
	}
	midnights_copied.push(total_copied_aggr[idx_2]-total_copied_aggr[idx_1])
    }
    
    var midnights_time_final = midnights_time.slice(0,-1);
    var midnights_copied_final = midnights_copied.slice(0,-1);
    var midnights_requested_final = [];

    //Pushes aggregate data values into list if the calendar date of their request  matches that one of the last 5 days, else pushes 0
    
    for (j = 0; j < midnights_time_final.length; j++){
        var day_found = false;
        var date1 = new Date(midnights_time_final[j]*1000);//date-time type object from unix timestamp                                                                                                                    
        
	for (i = 0; i < daily_requested_date.length; i++){
            var date2 =  new Date(daily_requested_date[i]*1000);//date-time type object from unix timestamp 

            if (date1.toDateString() === date2.toDateString()){//checks whether the date-time obbjects occurred on the same calendar date
                day_found = true;
                midnights_requested_final.push(daily_requested_size[i]/(1e+12));
            }
        }
        if (day_found==false)
            midnights_requested_final.push(0);
    }

    var trace_midnight_copied = {
	name: 'Daily copied',
	x: midnights_time_final.map(timeConverter),
	y: midnights_copied_final,
	type: 'bar',
	xaxis: 'x2',
	yaxis: 'y2',
	marker: {
	    color: 'rgba(57, 106, 173, 0.6)',
	}

    };

    var trace_midnight_requested = {
        name: 'Daily requested',
        x: midnights_time_final.map(timeConverter),
        y: midnights_requested_final,
        type: 'bar',
        xaxis: 'x2',
        yaxis: 'y2',
        marker: {
            color: 'rgba(57, 106, 173, 0.25)',
        }

    };
   
    var array = [];
    array = [trace_midnight_copied, trace_midnight_requested];
    return array;
   
}



//New version of Summary Graph: adapted for use with graphing.py and Compare tab
function drawSummary(data,serviceId){

    var traces = []; // every trace that will be plotted ends up in here
    
    var total_time = []; // time entry that spans all five days
    var total_total_white = []; // dummy that is set to 0 for all the time entries. Cosmetic Reasons.
    var total_total = []; // target volume of incomplete transfers per time
    var total_copied = []; // currently copied volume of incomplete transfers per time
    
    var total_total_phedex = []; // same for phedex 
    var total_copied_phedex = []; // ...
    var total_time_phedex = []; // ...
    var total_total_white_phedex = []; // ...


    var color_low = "rgba(0,103,112,.2)";
    var color_high = "rgba(0,103,112,.6)";

    var trace_midnight_copied = []; // what has been copied per day
    var trace_midnight_requested = []; // what has been requested per day


    total_time = data[0][0];
    total_copied = data[0][1];
    total_total = data[0][2];

    var total_total_white = [];

    for (var i =0; i<total_total.length; i++){
        total_total_white.push(0);
    }

    while ((total_time[total_time.length-1]-18000) % (86400) != 0){
        total_time.push(total_time[total_time.length-1]+900);
        total_total_white.push(0);

    }

    while (total_time[0]<total_time[total_time.length-1]-5*24*60*60*1.02){
	total_time.splice(0,1);
	total_copied.splice(0,1);
	total_total.splice(0,1);
	total_total_white.splice(0,1);
    }

    //retrieve data for bar graphs
    if(serviceId == 1)
	trace_midnight_requested = drawAggregates(total_time, total_copied, total_total)[1];
    if(serviceId == 2){
	color_low = "rgba(0,180,0,.2)";
	color_high = "rgba(0,180,0, .6)";
    };
    if(serviceId != 3)
	trace_midnight_copied = drawAggregates(total_time, total_copied, total_total)[0];

    traces.push(makeTrace("Time", total_time.map(timeConverter), total_total_white, 'solid', 2, false, "rgba(0,0,0,0)"));
    traces.push(makeTrace("Copied", total_time.map(timeConverter), total_copied, 'dot', 2, true, color_low));
    traces.push(makeTrace("Total", total_time.map(timeConverter), total_total, 'solid', 2, true, color_high));    

    //Compare:
    if(serviceId == 3){
	
	total_copied_phedex = data[1][1];
	total_time_phedex = data[1][0];
	total_total_phedex = data[1][2];	
	
	var total_total_white_phedex = [];

	for (var i =0; i<total_total_phedex.length; i++){
	    total_total_white_phedex.push(0);
	}

	while ((total_time_phedex[total_time_phedex.length-1]-18000) % (86400) != 0){
	    total_time_phedex.push(total_time_phedex[total_time_phedex.length-1]+900);
	    total_total_white_phedex.push(0);
	    
	}
	
	while (total_time_phedex[0]<total_time_phedex[total_time_phedex.length-1]-5*24*60*60*1.02){
	    total_time_phedex.splice(0,1);
	    total_copied_phedex.splice(0,1);
	    total_total_phedex.splice(0,1);
	    total_total_white_phedex.splice(0,1);
	}
	
	var total_sum = [];
	var total_sum_copied = [];
	
	for (var i = 0; i < total_total_phedex.length; i++){
	    total_sum.push(total_total_phedex[i] + total_total[i]);
	    total_sum_copied.push(total_copied_phedex[i] + total_copied[i]);
	}
	
	traces.push(makeTrace("Time", total_time_phedex.map(timeConverter), total_total_white_phedex, 'solid', 2, false, "rgba(0,0,0,0)"));
	traces.push(makeTrace("Other Copied", total_time_phedex.map(timeConverter), total_copied_phedex, 'dot', 2, true, "rgba(0,180,0,.2)"));
	traces.push(makeTrace("Other Total", total_time_phedex.map(timeConverter), total_total_phedex, 'solid', 2, true, "rgba(0,180,0,.6)"));

	traces.push(makeTrace("Sum Total", total_time_phedex.map(timeConverter), total_sum, 'solid', 2, true, "rgba(100, 0, 50, .6)"));
	traces.push(makeTrace("Sum Copied", total_time_phedex.map(timeConverter), total_sum_copied,'dot', 2, true, "rgba(100, 0, 50, .2)"));
	
    }
	
    var max_val = [0, 1.4*Math.max(Math.max(...total_total))];

    if (serviceId == 3)
	max_val = [0, 1.4*Math.max(Math.max(...total_total),Math.max(...total_sum))];
	
    var layout = {
	xaxis: {
	    domain: [0, 1.],
	    title: 'Time',
	    //tickformat: "%a %I%p",
	    tickangle: 45
	},
	yaxis: {title: 'Volume (TB)', range: max_val},
	margin: {t: 50, b: 160, l: 90, r: 0},
	hoverinfo: 'none',
	showlegend:true,
	barmode: 'group',
	title: 'Cumulative ongoing transfer requests overview',
	legend: {
	    x: 0,
	    y: 1.00,
	    bgcolor: "#E2E2E2",
	    
	    orientation: 'h',
	    traceorder: 'reversed',
	    font: {
		family: 'sans-serif',
		size: 16,
		color: '#000'
	    },
	},
	font: {
	    family: 'sans-serif',
	    size: 14,
	    color: '#7f7f7f'
	},
	paper_bgcolor: 'rgba(0,0,0,0)',
	plot_bgcolor: 'rgba(0,0,0,0)'
    };
    
    Plotly.newPlot('Summary', traces, layout);

}
 
function setServices(data){
    var servicesNav = d3.select('#services');
    servicesNav.selectAll('.serviceTab')
        .data(data)
        .enter().append('div').classed('serviceTab', true)
        .text(function (d) { return d.name; })
        .attr('id', function (d) { return 'service' + d.id; })
        .on('click', function (d) { window.location.assign(window.location.protocol + '//' + window.location.hostname + window.location.pathname + '?serviceId=' + d.id); });
    servicesNav.select(':last-child').classed('last', true);

}

//Draws Site Overviews: Updated to send user to new webpage (phedex_url) that shows transfers to node. (Slider graphs ported to phedex_url site)
function drawSiteOverview(data,serviceId) { 

    var T2entries = [];
    var addT2data = function(name, total, copied, nreplicas, problematic, phedex, stuck_total, stuck_copied){
	T2entries.push({name: name, total: total, copied: copied, nreplicas: nreplicas, problematic: problematic, url: url, stuck_total: stuck_total, stuck_copied: stuck_copied})
    };

    for(var i=0; i != data.length; i++){
	problematic = false;

	site = data[i];
	sitename = data[i][0];
	nreplicas = data[i][1];
	total_total = data[i][2];
	total_copied = data[i][3];
	stuck_total = data[i][4];
	stuck_copied = data[i][5];

	if (stuck_total != 0){
	    problematic = true;
	}

	url = 'http://dynamo.mit.edu/dynamo/dealermon/sites.php?site='+sitename + '&serviceId=' +serviceId;
	
	addT2data(sitename, total_total,total_copied, nreplicas, problematic, url, stuck_total, stuck_copied);

    }

    var data_name = [];
    var data_total = [];
    var data_total_stuck = [];
    var data_total_subtract_copied = [];
    var data_missing_stuck = [];
    var data_missing_not_stuck = [];
    var data_copied = [];
    var data_copied_stuck = [];
    var data_copied_not_stuck = [];
    var data_nreplicas = [];
    var data_problematic = [];
    var data_url = [];
    
    for (var it2 in T2entries){
	data_name.push(T2entries[it2]['name']);

	data_total.push(T2entries[it2]['total']/1e+12);
	data_total_stuck.push(T2entries[it2]['stuck_total']/1e+12);
	data_total_subtract_copied.push(T2entries[it2]['total']/1e+12-T2entries[it2]['copied']/1e+12);

	data_missing_stuck.push(T2entries[it2]['stuck_total']/1e+12-T2entries[it2]['stuck_copied']/1e+12);
	data_missing_not_stuck.push((T2entries[it2]['total']/1e+12-T2entries[it2]['stuck_total']/1e+12)-(T2entries[it2]['copied']/1e+12-T2entries[it2]['stuck_copied']/1e+12));

	data_copied.push(T2entries[it2]['copied']/1e+12);
	data_copied_stuck.push(T2entries[it2]['stuck_copied']/1e+12);
	data_copied_not_stuck.push(T2entries[it2]['copied']/1e+12-T2entries[it2]['stuck_copied']/1e+12);

	if (T2entries[it2]['nreplicas']!=0)
	    data_nreplicas.push("Replicas being copied:" + " " +T2entries[it2]['nreplicas'].toString());
	else 
	    data_nreplicas.push("");
	data_problematic.push(T2entries[it2]['problematic']);
	data_url.push(T2entries[it2]['url']);
    }

    // getting indices
    var len = data_total.length;
    var indices = new Array(len);
    for (var i = 0; i < len; ++i) indices[i] = i;
    
    // sort sites according to overall volume of ongoing transfers
    indices.sort(function (a, b) { return data_total[a] > data_total[b] ? -1 : data_total[a] < data_total[b] ? 1 : 0; });

    
    var data_name_sorted = [];
    var data_total_sorted = [];
    var data_total_stuck_sorted = [];
    var data_total_subtract_copied_sorted = [];
    var data_missing_stuck_sorted = [];
    var data_missing_not_stuck_sorted = [];
    var data_copied_stuck_sorted = [];
    var data_copied_not_stuck_sorted = [];
    var data_nreplicas_sorted = [];
    var data_url_sorted = [];    

    var indices_tape = [];
    var indices_d = [];
    var indices_t = [];

    for (var i = 0; i < len; i++){
	if (data_total[indices[i]]!=0){
	    if (!data_name[indices[i]].includes("MSS")){
		indices_tape.push(indices[i]);
		indices_d.push(indices[i]);
	    }
	}
    }

    var number_tape_sites = 0;
    var max_tape = 0;
    
    for (var i = 0; i < len; i++){
	if (data_total[indices[i]]!=0){
	    if (data_name[indices[i]].includes("MSS")){
		number_tape_sites += 1;
		indices_tape.push(indices[i]);
		indices_t.push(indices[i]);
		if (data_total[indices[i]] > max_tape){
		    max_tape = data_total[indices[i]];
		}
	    }
	}
    }

    final_indices = [];

    if (serviceId == 1){
	final_indices = indices;
    }
    else if (serviceId == 2 || serviceId == 3){
	for (var j = 0; j < indices_d.length; j++){
	    final_indices.push(indices_d[j]);
	}
    }
    else {
	for (var j = 0; j < indices_t.length; j++){
            final_indices.push(indices_t[j]);
        }
    }
    
    len = final_indices.length;

    for (var i = 0; i < len; i++){

	if (data_total[final_indices[i]]!=0){	    
	    data_name_sorted[i] = data_name[final_indices[i]];
	    data_total_sorted[i] = data_total[final_indices[i]];
	    data_total_stuck_sorted[i] = data_total_stuck[final_indices[i]];
	    data_total_subtract_copied_sorted[i] = data_total_subtract_copied[final_indices[i]];
	    data_missing_stuck_sorted[i] = data_missing_stuck[final_indices[i]];
	    data_missing_not_stuck_sorted[i] = data_missing_not_stuck[final_indices[i]];
	    data_copied_stuck_sorted[i] = data_copied_stuck[final_indices[i]];
	    data_copied_not_stuck_sorted[i] = data_copied_not_stuck[final_indices[i]];
	    data_nreplicas_sorted[i] = data_nreplicas[final_indices[i]];
	    data_url_sorted[i] = data_url[final_indices[i]];
	}
    }

    var data_sorted = [
		       data_name_sorted, 
		       data_total_subtract_copied_sorted, 
		       data_total_sorted, 
		       data_nreplicas_sorted, 
		       data_url_sorted,
		       data_copied_not_stuck_sorted,
		       data_copied_stuck_sorted,
		       data_missing_not_stuck_sorted,
		       data_missing_stuck_sorted
		       ];
    
    var data_copied_not_stuck_plot = {
	x: data_sorted[0],
	y: data_sorted[5],
	xaxis: 'x2',
	yaxis: 'y2',
	name: 'Copied moving',
	showlegend: false,
	marker: {
	    color: 'rgba(0, 103, 113, 0.25)',
	    line: {
		color: 'rgba(0, 103, 113, 0.5)',
		width: 1.0
	    }
	},
	
	type: 'bar'
    };

    var data_copied_stuck_plot = {
	x: data_sorted[0],
	y: data_sorted[6],
	xaxis: 'x2',
	yaxis: 'y2',
	name: 'Copied stuck',
	showlegend: false,
	marker: {
	    color: 'rgba(255,0,0,0.3)',
	    line: {
		color: 'rgba(255,0,0,0.5)',
		width: 1.0
	    }
	},
	
	type: 'bar'
    };

    // Dummies for legend
    var data_dummy = {
	x: data_sorted[0],
	y: data_sorted[5],
	name: 'Copied',
	marker: {
	    color: 'rgba(0, 103, 113, 0.25)',
	    line: {
		color: 'rgba(0, 103, 113, 0.25)',
		width: 1.0
	    }
	},
	
	type: 'bar'
    };

    var data_dummy_2 = {
	x: data_sorted[0],
	y: data_sorted[5],
	name: 'Missing',
	marker: {
	    color: 'rgba(0, 103, 113, 0.6)',
	    line: {
		color: 'rgba(0, 103, 113, 0.6)',
		width: 1.0
	    }
	},
	
	type: 'bar'
    };

    var data_total_not_stuck_plot = {
	x: data_sorted[0],
	y: data_sorted[7],
	xaxis: 'x2',
	yaxis: 'y2',
	name: 'Missing moving',
	text: data_sorted[3],
	showlegend: false,
	marker: {
	    color: 'rgba(0, 103, 113, 0.6)',
	    line: {
		color: 'rgba(0, 103, 113, 0.6)',
		width: 1.0
	    }
	},
	type: 'bar'
    };

    var data_total_stuck_plot = {
	x: data_sorted[0],
	y: data_sorted[8],
	xaxis: 'x2',
	yaxis: 'y2',
	name: 'Missing stuck',
	showlegend: false,
	marker: {
	    color: 'rgba(255,0,0,1.0)',
	    line: {
		color: 'rgba(255,0,0,1.0)',
		width: 1.0
	    }
	},
	type: 'bar'
    };
    
    var data = [data_copied_stuck_plot, data_copied_not_stuck_plot, data_total_stuck_plot,data_total_not_stuck_plot,data_dummy,data_dummy_2];


    var service = " Dynamo + Other";
    var tapestring = "";
    var tape_xposition = 1-(1.0+0.03*indices.length)*number_tape_sites/(indices.length);
    if (number_tape_sites == 0){
	tapestring = "";
    }

    if (serviceId == 2)
	service = " Other";
    if (serviceId == 4)
	service = " Tape";
    if (serviceId == 1){
	service = " Dynamo";
	tapestring = "";
    }
    var layout = {
	annotations: 
	[{
		xref: 'paper',
		yref: 'paper',
		x: 0.5,
		xanchor: 'left',
		y: 0.94,
		yanchor: 'bottom',
		text: 'Stuck transfers <br>(<1% within 5 days)',
		showarrow: false,
		font: {
		    family: 'sans-serif',
		    size: 16,
		    color: '#000'
		}
	    },   
		{
		    xref: 'paper',
		    yref: 'paper',
		    x: tape_xposition,
		    xanchor: 'left',
		    y: max_tape/(1.3*Math.max(...data_sorted[2])),
		    yanchor: 'top',
		    text: tapestring,
		    textangle: 270,
		    showarrow: false,
		    font: {
			family: 'sans-serif',
			size: 16,
			color: '#000'
		    }
		},    	    
    {
	x: 0.48,
	y: 0.975,
	xref: 'paper',
	yref: 'paper',
	text: '',
	showarrow: true,
	arrowhead: 7,
	arrowsize: 2,
	ax: 0,
	ay: -5,
	arrowcolor:'rgba(255,0,0,1)'
    }],
	xaxis2: {domain: [0, 1],
		 anchor: 'y2',
		 tickformat: " %I%p",
		 tickangle: 45,
		 utorange: true,
		 zeroline: true,
		 showline: false,
		 autotick: true,
		 ticks: '',
		 showticklabels: true
	},
	yaxis2: {domain: [0, 1],
		 anchor: 'x2',
		 range: [0, 1.3*Math.max(...data_sorted[2])],
		 utorange: true,
		 zeroline: false,
		 showline: false,
		 autotick: true,
		 ticks: '',
		 showticklabels: false
	},
	
	
	xaxis: {
	    tickangle: 45,
	    //showticklabels: false
	},
	yaxis: {
	    range: [0, 1.3*Math.max(...data_sorted[2])],
	    title: 'Volume being copied to site (TB)'
	},
	margin: {t: 50, b: 160, l: 90, r: 40},
	title: 'Status of sites with ongoing transfers:' + service ,
	
	barmode: 'stack',
	legend: {
	    x: 0,
	    y: 1.00,
	    bgcolor: "#E2E2E2",
	    
	    orientation: 'h',
	    traceorder: 'reversed',
	    font: {
		family: 'sans-serif',
		size: 16,
		color: '#000'
	    },
	},
	font: {
	    family: 'sans-serif',
	    size: 14,
	    color: '#7f7f7f'
	},
	shapes:    {
	    type: 'line',
	    xref: 'paper',
	    yref: 'paper',
	    x0: 0,
	    y0: 0.3,
	    x1: 0.7,
	    y1: 0.6,
	    line: {
		color: 'rgba(50, 171, 96, 0.4)',
		width: 5,
	    }
	}
    };
    
    var siteoverview = document.getElementById('SiteOverview');

    Plotly.newPlot('SiteOverview', data, layout);
    
    siteoverview.on('plotly_click', function(data){
	    if(data.points.length === 4) {
		var link = data_url_sorted[data.points[0].pointNumber];		
		window.open(link,"_blank");
	    }
	});
    
}





