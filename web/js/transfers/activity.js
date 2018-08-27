function initPage(graph,entity,src_filter,dest_filter,no_mss,period,upto,exit_code)
{
  var jaxInput = {
    'url': dataPath + '/transfers/history',
    'data': { 'graph': graph,
	      'entity': entity,
	      'src_filter': src_filter,
	      'dest_filter': dest_filter,
	      'no_mss': no_mss,
	      'period': period,
	      'upto': upto,
	      'exit_code': exit_code
    },
    'dataType': 'json',
    'success': function (data, textStatus, jqXHR) { displayHistogram(graph,entity,data.data); },
    'error': handleError,
    'async': false
  };
  $.ajax(jaxInput);

  // make sure to conserve the values in the form typed in so far
  
  // select type
  $('#graph > option[value="' + graph + '"]').attr('selected', true);
  $('#entity > option[value="' + entity + '"]').attr('selected', true);
  $('#period > option[value="' + period + '"]').attr('selected', true);
  $('#no_mss > option[value="' + no_mss + '"]').attr('selected', true);
  // text type
  $('#src_filter').attr('value',src_filter);
  $('#dest_filter').attr('value',dest_filter);
  $('#upto').attr('value',upto);
  $('#exit_code').attr('value',exit_code);
}

function displayHistogram(graph,entity,data)
{
  // defaults
  var dt = 3600000; // 1 hour in milliseconds
  var summary_string = 'undefined';
  var timing_string = 'undefined';
  var title = 'undefined';
  var subtitle = 'undefined';
  var yaxis_label = "";

  // get information out of the container
  if ("0" in data) {
    summary_string = data[0].summary_string;
    timing_string = data[0].timing_string;
    title =  data[0].title;
    subtitle =  data[0].subtitle;
    yaxis_label = data[0].yaxis_label
    if ("data" in data[0]) {
      dt = data[0].data[1].time*1000. - data[0].data[0].time*1000.;
    }
  }

  // get the plot data
  var plot_data = [];
  for (var i_site in data) {
    var site_data = data[i_site].data;
    var plot_datum = {
      x: [],
      y: [],
      name: data[i_site].name,
      marker: {
  	opacity: 1.0,
  	line: {
  	  color: 'rbg(107,48,107)',
  	  width: 1.5,
  	}
      },
      type: 'bar',
    };
    for (var i in site_data) {
      var row = site_data[i];
      var date = new Date(row.time*1000); // input in epoch milliseconds
      date = date.getTime() + dt/2;

      var dateX = new Date(date);
      var size = row.size/1000/1000/1000;
      plot_datum.x.push(dateX);
      plot_datum.y.push(size);
    }
    plot_data.push(plot_datum);
  }

  // define the basic plot layout
  var basic_layout = {
    autosize: false, width: 900, height: 600,
    margin: { l: 80, r: 10, t: 90, b: 80 },
    title: '',
    titlefont: { family: 'Arial, sans-serif', size: 28, color: '#444444' },
    showlegend: true,
    xaxis: {
      title: 'Time Axis',
      titlefont: { family: 'Arial, sans-serif', size: 24, color: '#444444' },
      tickfont: { family: 'Arial, sans-serif',  size: 16, color: 'black' },
    },	
    yaxis: {
      title: yaxis_label,
      titlefont: { family: 'Arial, sans-serif', size: 24, color: '#444444' },
      tickfont: { family: 'Arial, sans-serif',  size: 20, color: 'black' },
      ticklen: 0.5,
    },
    bargap: 0,
    barmode: 'stack',
    hovermode: 'closest',
    annotations: [{
  	xref: 'paper',
  	yref: 'paper',
  	xanchor: 'center',
  	yanchor: 'bottom',
  	x: 0.5,
  	y: 1.08, 
  	font: {
  	  family: 'arial, sans-serif',
  	  size: 34,
  	  color: '#444444',
  	},
  	text: title,
  	showarrow: false,
      },{
  	xref: 'paper',
  	yref: 'paper',
  	xanchor: 'center',
  	yanchor: 'bottom',
  	x: 0.5,
  	y: 1.03, 
  	font: {
  	  family: 'sans-serif',
  	  size: 16,
  	  color: 'gray',
  	},
  	text: subtitle,
  	showarrow: false,
      },{
  	xref: 'paper',
  	yref: 'paper',
  	xanchor: 'center',
  	yanchor: 'bottom',
  	x: 0.5,
  	y: 0.98, 
  	font: {
  	  family: 'sans-serif',
  	  size: 16,
  	  color: '#440000',
  	},
  	text: summary_string,
  	showarrow: false,
      },{
  	xref: 'paper',
  	yref: 'paper',
  	xanchor: 'left',
  	yanchor: 'bottom',
  	x: -0.12,
  	y: -0.17, 
  	font: {
  	  family: 'sans-serif',
  	  size: 12,
  	  color: '#004400',
  	},
  	text: timing_string,
  	showarrow: false,
      }],
  };

  var layout = $.extend( true, {}, basic_layout );
  Plotly.newPlot('activity', plot_data, layout);
}
