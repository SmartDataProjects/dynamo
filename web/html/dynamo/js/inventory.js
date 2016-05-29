var colors = [
   '#d5ff00',
   '#00ff00',
   '#0000ff',
   '#ff0000',
   '#01fffe',
   '#ffa6fe',
   '#ffdb66',
   '#006401',
   '#010067',
   '#95003a',
   '#007db5',
   '#ff00f6',
   '#ffeee8',
   '#774d00',
   '#90fb92',
   '#0076ff',
   '#ff937e',
   '#6a826c',
   '#ff029d',
   '#fe8900',
   '#7a4782',
   '#7e2dd2',
   '#85a900',
   '#ff0056',
   '#a42400',
   '#00ae7e',
   '#683d3b',
   '#bdc6ff',
   '#263400',
   '#bdd393',
   '#00b917',
   '#9e008e',
   '#001544',
   '#c28c9f',
   '#ff74a3',
   '#01d0ff',
   '#004754',
   '#e56ffe',
   '#788231',
   '#0e4ca1',
   '#91d0cb',
   '#be9970',
   '#968ae8',
   '#bb8800',
   '#43002c',
   '#deff74',
   '#00ffc6',
   '#ffe502',
   '#620e00',
   '#008f9c',
   '#98ff52',
   '#7544b1',
   '#b500ff',
   '#00ff78',
   '#ff6e41',
   '#005f39',
   '#6b6882',
   '#5fad4e',
   '#a75740',
   '#a5ffd2',
   '#ffb167',
   '#009bff',
   '#e85ebe'
];

function initPage(dataType, categories, constraints) {
    $('#dataType > option[value="' + dataType + '"]')
        .attr('selected', 'selected');

    $('#categories > option[value="' + categories + '"]')
        .attr('selected', 'selected');

    for (var c in constraints) {
        $('#' + c).attr('value', constraints[c]);
    }

    loadData();
}

function limitOptions() {
    if ($('#dataType').val() == 'replication') {
        $('#categories > option[value="sites"]')
            .attr('selected', '')
            .attr('disabled', 'disabled');
        $('#categories > option[value="groups"]')
            .attr('selected', '')
            .attr('disabled', 'disabled');
        $('#site')
            .attr('value', '')
            .attr('disabled', 'disabled');
    }
}

function replicationMarker(datum, idx) {
    var r = 2;
    var s = 3;
    var cy = 3;
    var svg = '<text x="20" y="0" text-anchor="end">' + datum.key + '</text>';
    for (var i = 0; i < int(datum.value); ++i)
        svg += '<circle cy="' + cy + '" cx="' + (22 + s * i) + '" fill="' + colors[idx] + '"/>';

    var cx = 22 + s * int(datum.value);
    var phi = (datum.value - Math.floor(datum.value)) * Math.PI;
    svg += '<path d="';
    svg += 'm ' + cx + ' ' + cy;
    svg += ' l ' + (cx + r * cos(phi)) + ' ' + (cy - r * sin(phi));
    svg += ' a ' + r + ' ' + r + ' 0 1 ' + (cx + r * cos(phi)) + ' ' + (cy + r * sin(phi));
    svg += ' z" fill="' + colors[idx] + '"/>';

    return svg;
}

function displayData(data) {
    if (data.dataType == 'size') {
        var graphData = data.content.slice(0, colors.length);
        var residuals = data.content.slice(colors.length);
        if (residuals.length != 0) {
            var remaining = 0;
            for (var i in residuals)
                remaining += residuals[i].value;
            graphData.push({key: 'Others', value: remaining});
        }

        var total = 0;
        for (var i in graphData) {
            total += graphData[i].value;
        }
    
        var arcpath = d3.svg.arc()
            .outerRadius(30)
            .innerRadius(0);

        var pie = d3.layout.pie()
            .sort(null)
            .value(function (d) { return d.value; });

        var graph = d3.select('#graph')
            .attr('viewBox', '-35 -35 70 100');

        graph.selectAll('.velem').remove();

        graph.selectAll('.velem')
            .data(pie(graphData))
            .enter()
            .append('path').classed('velem', true)
            .attr('d', arcpath)
            .attr('fill', function (d, i) { return colors[i]; })
            .attr('stroke', 'white')
            .attr('stroke-width', function (d) { if (d.data.value < total * 0.01) return 0; else return 0.1; });

        var legend = d3.select('#legend')
            .attr('height', 20 * graphData.length) // 20 px per row
            .attr('viewBox', '0 0 30 ' + (2 * graphData.length)); // svg coordinate is 1/10 of browser

        legend.selectAll('.legendEntry').remove();

        var entries = legend.selectAll('.legendEntry')
            .data(graphData)
            .enter().append('g').classed('legendEntry', true)
            .attr('transform', function (d, i) { return 'translate(0,' + (2 * i) + ')'; });

        entries.append('circle')
            .attr('cx', 1.)
            .attr('cy', 1.)
            .attr('r', 0.9)
            .attr('fill', function (d, i) { return colors[i]; });

        entries.append('text')
            .attr('font-size', 2)
            .attr('dx', 4)
            .attr('dy', 1.6)
            .text(function (d) { return d.key; });
    }
    else if (data.dataType == 'replication') {
        var graph = d3.select('#graph')
            .attr('viewBox', '-35 -35 70 ' + (20 * data.content.length));

        graph.selectAll('.velem').remove();

        graph.selectAll('.velem')
            .data(data.content)
            .enter()
            .append('g').classed('velem', true)
            .innerHTML(replicationMarker);

        d3.select('#legend').selectAll('.legendEntry').remove();
    }
}

function loadData() {
    var inputData = {
        ajax: 1,
        dataType: $('#dataType').val(),
        categories: $('#categories').val(),
        campaign: $('#campaign').val(),
        dataTier: $('#dataTier').val(),
        dataset: $('#dataset').val(),
        site: $('#site').val(),
        group: $('#group').val()
    };

    $.get('inventory.php', inputData, function (data, textStatus, jqXHR) { displayData(data); }, 'json');
}
