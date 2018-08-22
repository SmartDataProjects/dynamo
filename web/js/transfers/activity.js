function initPage(graph,entity,src_filter,dest_filter,no_mss,period,upto)
{
  var jaxInput = {
    'url': dataPath + '/transfers/history',
    'data': { 'graph': graph,
	      'entity': entity,
	      'src_filter': src_filter,
	      'dest_filter': dest_filter,
	      'no_mss': no_mss,
	      'period': period,
	      'upto': upto
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
}

function displayHistogram(graph,entity,data)
{
  // defaults
  var dt = 3600000; // 1 hour in milliseconds
  var timing_string = 'undefined';
  var title = 'undefined';
  var subtitle = 'undefined';

  // get information out of the container
  if ("0" in data) {
    timing_string = data[0].timing_string;
    title =  data[0].title;
    subtitle =  data[0].subtitle;
    if ("data" in data[0]) {
      dt = data[0].data[1].time*1000 - data[0].data[0].time*1000;
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
    margin: { l: 80, r: 10, t: 60, b: 80 },
    title: '',
    titlefont: { family: 'Arial, sans-serif', size: 28, color: '#444444' },
    showlegend: true,
    xaxis: {
      title: 'Time Axis',
      titlefont: { family: 'Arial, sans-serif', size: 24, color: '#444444' },
      tickfont: { family: 'Arial, sans-serif',  size: 16, color: 'black' },
    },	
    yaxis: {
      title: 'undefined plot',
      titlefont: { family: 'Arial, sans-serif', size: 24, color: '#444444' },
      tickfont: { family: 'Arial, sans-serif',  size: 20, color: 'black' },
      ticklen: 0.5,
    },
    bargap: 0,
    barmode: 'stack',
    annotations: [{
  	xref: 'paper',
  	yref: 'paper',
  	xanchor: 'center',
  	yanchor: 'bottom',
  	x: 0.5,
  	y: 1.05, 
  	font: {
  	  family: 'arial, sans-serif',
  	  size: 30,
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
  	y: 0.99, 
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
  	xanchor: 'left',
  	yanchor: 'bottom',
  	x: -0.12,
  	y: -0.17, 
  	font: {
  	  family: 'sans-serif',
  	  size: 12,
  	  color: 'green',
  	},
  	text: timing_string,
  	showarrow: false,
      }],
  };

  // adjust x-axis labels
  if (graph == 'rate') {
    basic_layout['yaxis']['title'] = 'Transfered Rate [GB/sec]'
  }
  if (graph == 'volume') {
    basic_layout['yaxis']['title'] = 'Transfered Volume [GB]'
  }
  if (graph == 'cumulative') {
    basic_layout['yaxis']['title'] = 'Cumulative Transfered Volume [GB]'
  }

  var layout = $.extend( true, {}, basic_layout );
  Plotly.newPlot('activity', plot_data, layout);
}
