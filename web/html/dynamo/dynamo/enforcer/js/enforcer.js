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
    var time = year + '-' + month + '-' + date + ' ' + hour + ':' + min + ':' + sec +'0.000000';
    return time;
}


function initPage(rulename) {

    var ajaxInput_status = {
	'url': 'http://dynamo.mit.edu/dynamo/enforcer/main.php',
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
    var there = []; 
    var considered_sites = [];
    var considered_backends = [];
    var considered_lat = [];
    var considered_long = [];
    var title = "";
    var line_number = "";
    var last_missing = 0;
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
	    times = obj['data'][j].date.map(timeConverter);
	    missing = obj['data'][j].missing;
	    there = obj['data'][j].there;
	    considered_sites = obj['data'][j].sites;
	    considered_backends = obj['data'][j].backends;
	    considered_lat = obj['data'][j].lat;
	    considered_long = obj['data'][j].long;
	    counter += 1;
        }
	last_missing = missing[missing.length-1];
	last_there = there[there.length-1];
    }
  
    while (times[0]<times[times.length-1]-5*24*60*60*1.02){
        times.splice(0,1);
        missing.splice(0,1);
        there.splice(0,1);
    }


    var state = [
	    {
		x: ["Missing", "There"],
		y: [last_missing,last_there],
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
	title: "Rule: " + '<a href="https://github.com/SmartDataProjects/dynamo-policies/blob/master/common/enforcer_rules.json#L'+line_number+'">'+title+'</a>',
	yaxis: {
	    title: '# of datasets',
	    titlefont: {
		size: 18,
		color: '#7f7f7f'
	    }
	}
    };

    Plotly.newPlot('Bars', state, layout1);

    var trend = [
	    {
		x: times,
		y: missing,
		type: 'scatter',
		mode: 'lines',
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
	    title: 'Datasets missing',
	    titlefont: {
		size: 18,
		color: '#7f7f7f'
	    }
	}
    };

    Plotly.newPlot('Reason', trend, layout2);

    scattergeomarkers(considered_sites, considered_backends, considered_long, considered_lat);

}
