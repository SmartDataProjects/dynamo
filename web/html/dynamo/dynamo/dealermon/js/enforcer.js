// globally defined variables

function timeConverter(UNIX_timestamp){
    var a = new Date(UNIX_timestamp * 1000);
    var months = ['01','02','03','04','05','06','07','08','09','10','11','12'];
    var year = a.getFullYear();
    var month = months[a.getMonth()];
    var date = a.getDate();
    var hour = a.getHours();
    var min = a.getMinutes();
    var sec = a.getSeconds();
    var time = year + '-' + month + '-' + date + ' ' + hour + ':' + min;// + ':' + sec +'0.000000';
    if (min == "0")
	time = time + "0";
    return time;
}


function initPage(rulename) {

    var ajaxInput_status = {
	'url': 'http://dynamo.mit.edu/dynamo/dealermon/enforcer.php',
        'data': {'getStatus': 1, 'whichRule': rulename},
        'success': function (data) { 
	    drawStatus(data);
	},
        'dataType': 'json',
        'async': false
    };

    $.ajax(ajaxInput_status);

}

function scattergeomarkers(considered_sites,considered_backends,considered_long,considered_lat) {

    var siteinfo = [];
    for (var i = 0; i < considered_sites.length; i++){
	siteinfo.push(considered_sites[i] + " (" + considered_backends[i] + ")");
    }

    var data = [{
	    type: 'scattergeo',
	    mode: 'markers',
	    text: siteinfo,
	    lon: considered_long,
	    lat: considered_lat,
	    marker: {
		size: 7,
		line: {
		    width: 1
		}
	    },
	    name: 'Considered sites',
	}];

    var layout = {
        title: 'Considered sites',
        font: {
            size: 6
        },
        titlefont: {
            size: 16
        },
        geo: {
            scope: 'world',
            resolution: 50,
            lonaxis: {
                'range': [Math.min(...considered_long)-10, Math.max(...considered_long)+10]
            },
            lataxis: {
                'range': [Math.min(...considered_lat)-10, Math.max(...considered_lat)+10]
            },
            showrivers: true,
            rivercolor: '#fff',
            showlakes: true,
            lakecolor: '#fff',
            showland: true,
            landcolor: 'rgba(28,200,225,.2)',
            countrycolor: '#d3d3d3',
            countrywidth: 1.5,
            subunitcolor: '#d3d3d3'
        }
    };

    Plotly.newPlot('WorldMap', data, layout);

}

function drawStatus(data) {

    var times = []; 
    var missing = []; 
    var enroute = []; 
    var there = []; 
    var considered_sites = [];
    var considered_backends = [];
    var considered_lat = [];
    var considered_long = [];
    var title = "";
    var line_number = "";
    var last_missing = 0;
    var last_enroute = 0;
    var last_there = 0;

    for(var i = 0 in data) {
        var site_ready_tmp = 0
        var obj = data[i];
	tmp = obj['rule'];

	var pos = tmp.lastIndexOf('_');
	title = tmp.substring(0,pos);

	line_number = tmp.substring(pos,tmp.length);
	line_number = line_number.replace("_","");

	var dates = 0;

	var counter = 0;
        for (var j in obj['data']){
	    times = obj['data'][j].date;
	    missing = obj['data'][j].missing;
	    enroute = obj['data'][j].enroute;
	    there = obj['data'][j].there;
	    considered_sites = obj['data'][j].sites;
	    considered_backends = obj['data'][j].backends;
	    considered_lat = obj['data'][j].lat;
	    considered_long = obj['data'][j].long;
	    counter += 1;
        }
	last_missing = missing[missing.length-1];
	last_enroute = enroute[enroute.length-1];
	last_there = there[there.length-1];
    }
  
    while (times[0]<times[times.length-1]-5*24*60*60*1.02){
        times.splice(0,1);
        missing.splice(0,1);
        enroute.splice(0,1);
        there.splice(0,1);
    }


    main_url = "http://dynamo.mit.edu/dynamo/dealermon/monitoring_enforcer/RULE_STATUS.csv"
    urls = []
    urls.push(main_url.replace("RULE",title).replace("STATUS","missing"))
    urls.push(main_url.replace("RULE",title).replace("STATUS","enroute"))
    urls.push(window.location.href);

    var missing_and_subscribed = [];

    for (var i = 0; i < missing.length; i++){
	missing_and_subscribed.push(missing[i]+enroute[i]);
    }

    var state = [
	    {
		x: ["Missing", "Subscribed" ,"Complete"],
		y: [last_missing,last_enroute,last_there],
		type: 'bar',
		marker: {
		    color: 'rgba(58,200,225,.5)',
		    line: {
			color: 'rbg(8,48,107)',
			width: 1.5
		    }
		}
	    }
	    ];

    var layout1 = {
	title: "Rule: " + '<a href="https://github.com/SmartDataProjects/dynamo-policies/blob/master/common/enforcer_rules_physics.json#L'+line_number+'">'+title+'</a>',
	yaxis: {
	    title: '# of datasets',
	    titlefont: {
		size: 18,
		color: '#7f7f7f'
	    }
	}
    };

    var bars = document.getElementById('Bars');

    Plotly.newPlot('Bars', state, layout1);

    bars.on('plotly_click', function(data){
            if(data.points.length === 1) {
                var link = urls[data.points[0].pointNumber];
                window.open(link,"_blank");
            }
        });

    times_converted = times.map(timeConverter);

    var trend = [
	    {
		x: times_converted,
		y: missing_and_subscribed,
		type: 'scatter',
		mode: 'lines',
		name: 'hv',
		line: {shape: 'hv'},
		marker: {
		    color: 'rgba(58,200,225,.5)',
		    line: {
			color: 'rbg(8,48,107)',
			width: 1.5
		    }
		}
	    }
	    ];

    var layout2 = {
	xaxis: {
	    title: 'Day',
	    titlefont: {
		size: 18,
		color: '#7f7f7f'
	    }
	},
	yaxis: {
	    title: 'Not complete',
	    titlefont: {
		size: 18,
		color: '#7f7f7f'
	    }
	}
    };

    Plotly.newPlot('Reason', trend, layout2);

    scattergeomarkers(considered_sites, considered_backends, considered_long, considered_lat);

}
