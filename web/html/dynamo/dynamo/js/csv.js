//Creates the new site referenced by phedex_url in tracker.js

var replicanames = [];

//Returns alternate format for the timestamps used on axes
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

//Communicates with sites.php to retrieve site specific data
function initPage(serviceId, site){

    var ajaxInput_sitecsvs = {
	'url': 'http://dynamo.mit.edu/dynamo/dealermon/sites.php',
	'data': {'getSiteCSVs': 1, 'serviceId': serviceId, 'site': site},
	'success': function (data) {
	    
	    makeTable(data, site, serviceId);

	},
	'dataType': 'json',
	'async': false
    };
    
    $.ajax(ajaxInput_sitecsvs);
   
}


//Creates HTML table with the information from Site CSV: links to PhEDEx API and progress graph
function makeTable(data, site, serviceId){
    
    document.getElementById("titleBox").innerHTML = "<h1>"+ site +"</h1>" + "<p1> Click Request ID for corresponding PhEDEx API.<br> Click Replica Name for corresponding progress graph.  </p1> <p1 style=color:#ff6666> <br>Red cells signify stuck transfers. (<1% copied in past 5 days) <p1>";
    var replicaTable = "<table><tr><th>Request ID:</th><th></th><th>Replica Name: </th><th>Copied (TB): </th><th>Total (TB): </th>"; 
    var nreplicas = 0;   
    
    for(var i in data){
	
	var replicaname = data[i][0];
	var id = data[i][1];
	var total = Math.round(10000*(data[i][2]/Math.pow(10,12)))/10000;
	var copied = Math.round(10000*(data[i][3]/Math.pow(10,12)))/10000;
	var isstuck = false;
	var DorP = replicaname.split("_")[0].charAt(0);
	var DorPcolor = "#2C8CFF";
	var color = "white";
	replicaname = replicaname.slice(1);
	if (DorP == "P"){
	    DorPcolor = "#07EF00";
	    DorP = "O";
	}

	if (data[i][4] == 1){
	    color = "#ff6666";
	    isstuck = true;
	}
	nreplicas +=1;

	replicaTable += "<tr><td>" + id.link('https://cmsweb.cern.ch/phedex/datasvc/perl/prod/blockarrive?to_node=' + site +'&block=/' + replicaname.replace("+","/").replace("+","/") +"%23*") + "</td><td bgcolor=" + DorPcolor+ ">" + DorP + "</td><td bgcolor=" + color + " id='textButton"+replicaname+"'  style='cursor: pointer'>"  + replicaname + "</td><td>" + copied + "</td><td>" + total + "</td></tr>";
	replicanames.push(replicaname);
	
    }
    replicaTable += "</table>";

    document.getElementById("tableBox").innerHTML= "Number of Replicas: " + nreplicas + "<br><br>" +replicaTable;

}

function makeGraphTrace(data, site, replicaname){
    

    var traces_sites = [];


    for(var k in data){

        for(var i in data[k]){
	    var obj = data[k][i];
            var sitename = obj.site;
	    /*
            if (site != sitename)
		continue;
	    */
            for(var j in obj["data"]){
		var replica = obj["data"][j];	
		var replicatograph  = replica.replica.substring(replica.replica.indexOf("_") + 1);
		/*
		if (replicatograph != replicaname){
		    continue;
		}
		*/

		traces_sites.push(makeTrace(replica.replica,time_converted,replica.ratio,'dot',1.6,false, "#ff6666"));
	    }
	}
    }	      
    
    return traces_sites;
}

function makeGraph(replicaname, serviceId, site){

    window.alert(site);

    var traces_sites = [];
    var echoo = "";
    var ajaxInput_grapher = { 
	'url': 'http://dynamo.mit.edu/dynamo/dealermon/sites.php',
	'data': {'getSiteRRDs': 1, 'serviceId': serviceId, 'site': site, 'replicaname': replicaname},
	'success': function (data) {
	    echoo = "Hi";
	    traces_sites = makeGraphTrace(data,site,replicaname);
	},
	'dataType': 'json',
	'async': false
    };
    
    $.ajax(ajaxInput_grapher);
    window.alert(echoo);

    var layout_sites = {
	title: replicaname,
	width: 700,
	height: 300,

	xaxis: {
	    //showgrid: false,
	    tickangle: 45
	},
	yaxis: {
	    title: 'Volume copied (%)',
	    range: [0, 105],
	    
	},
	margin: {t: 32, b: 50, l: 50, r: 10},
	hoverinfo: 'closest',
	
	
	barmode: 'stack',
	legend: {
	    x: 0.6720,
	    y: 1.00,
	    bgcolor: "#E2E2E2",
	    title: "GRAPH",
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
	    size: 12,
	    color: '#7f7f7f'
	}
    };


    Plotly.newPlot("graphModal1", traces_sites, layout_sites);
    
}

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
    
window.onload = function(){
    var url = window.location.href;
    var modal = document.getElementById('graphModal');
    var span = document.getElementsByClassName("close")[0];
    
    var site = url.substring(url.indexOf("=")+1,url.lastIndexOf("&"));
    var serviceId = url.charAt(url.length-1);
    var btns =[];


    span.onclick = function(){
	modal.style.display = "none";
    };
    window.onclick = function(event){
	if (event.target == modal){
	    modal.style.display = "none";
	}
    };
    for(var i =0; i<replicanames.length; i++)(function(i){
	    
	    btns.push(document.getElementById("textButton" + replicanames[i]));
	    var replicaname = replicanames[i];
	    
	    btns[i].onclick = function() {		

		makeGraph(replicaname,serviceId,site)
		modal.style.display = "block";
	    };

	})(i);
};
 
