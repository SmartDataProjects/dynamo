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
   '#e85ebe',
   '#dddddd'
];

var loading = new Spinner({'scale': 5, 'corners': 0, 'width': 2});

function initPage(dataType, categories, constraints) {
    var ajaxInput = {
        'url': 'inventory.php',
        'data': {'getGroups': 1},
        'success': function (data, textStatus, jqXHR) { setGroups(data); },
        'dataType': 'json',
        'async': false
    };

    $.ajax(ajaxInput);

    $('#dataType > option[value="' + dataType + '"]')
        .attr('selected', true);

    $('#categories > option[value="' + categories + '"]')
        .attr('selected', true);

    for (var c in constraints) {
        if (c == 'group') {
            for (var g in constraints[c])
                $('#group [value="' + constraints[c][g] + '"]').attr('selected', true);
        }
        else
            $('#' + c).attr('value', constraints[c]);
    }

    $('#dataType').change(limitOptions);
    $('#categories').change(limitOptions);
    $('.constraint').change(limitOptions);

    $(document).ajaxStart(function () {
            var graph = d3.select('#graph');
            graph.selectAll('.message').remove();
            graph.selectAll('.velem').remove();
            d3.select('#axis').selectAll('g').remove();
            d3.select('#legend').selectAll('.legendEntry').remove();

            loading.spin();
            $('#graphCont').append($(loading.el));
        });
    $(document).ajaxStop(function () {
            loading.stop();
        });
  
    loadData();

    limitOptions();
}

function limitOptions() {
    // do not allow datasets view unless some constraints are set
    $('#categories > option[value="datasets"]')
        .attr('disabled', $('#campaign').val() == '' && $('#dataset').val() == '' && $('#site').val() == '');

    var dataType = $('#dataType').val();

    if (dataType == 'replication' || dataType == 'usage') {
        var selected = $('#categories :selected').get(0);
        if (selected.value == 'sites')
            selected = $('#categories :first').get(0);
        
        $('#categories > option[value="sites"]')
            .attr('selected', false)
            .attr('disabled', true);
        
        if (dataType == 'replication')
            $('#site')
                .attr('value', '')
                .attr('disabled', true);
        else
            $('#site')
                .attr('disabled', false);

        selected.selected = true;
    }
    else {
        $('#categories > option[value="sites"]')
            .attr('disabled', false);
        $('#site')
            .attr('value', '')
            .attr('disabled', false);
    }

    if (dataType == 'replication') {
        $('#physicalText').html('Complete replicas');
        $('#projectedText').html('All replicas');
    }
    else {
        $('#physicalText').html('Physical size');
        $('#projectedText').html('Projected size');
    }
}

function setGroups(data) {
    d3.select('#group')
        .selectAll('option')
        .data(data)
        .enter()
        .append('option')
        .attr('value', function (d) { return d; })
        .text(function (d) { return d; });
}

function displayData(data) {
    var legendWidth = d3.select('#legendCont').node().clientWidth * 0.1;

    if (data.content.length == 0) {
        d3.select('#axisBox').style('height', '0');
        d3.select('#graphBox').style('height', '100%');
        d3.select('#graph')
            .attr('viewBox', '0 0 70 70')
            .append('text').classed('message', true)
            .attr('transform', 'translate(20,35)')
            .attr('font-size', 10)
            .attr('fill', '#bbbbbb')
            .text('Empty');

        var legend = d3.select('#legend')
            .attr('viewBox', '0 0 ' + legendWidth + ' 0');

        return;
    }

    if (data.dataType == 'size') {
        // data.content: [{key: (key_name), size: (size)}]

        d3.select('#axisBox').style('height', '8%');
        d3.select('#graphBox').style('height', '92%');

        var graphData = data.content.slice(0, colors.length - 1);
        var residuals = data.content.slice(colors.length - 1);
        if (residuals.length != 0) {
            var remaining = 0;
            for (var i in residuals)
                remaining += residuals[i].size;
            graphData.push({'key': 'Others', 'size': remaining});
        }

        var total = 0;
        for (var i in graphData) {
            total += graphData[i].size;
        }
    
        var arcGen = d3.svg.arc()
            .outerRadius(30)
            .innerRadius(0);

        var labelArcGen = d3.svg.arc()
            .outerRadius(30)
            .innerRadius(22);

        var pie = d3.layout.pie()
            .sort(null)
            .value(function (d) { return d.size; });

        d3.select('#axis')
            .attr('viewBox', '0 0 70 8')
            .append('g')
            .append('text')
            .attr('transform', 'translate(35, 3.9)')
            .attr('font-size', 3)
            .attr('text-anchor', 'middle')
            .text('Total: ' + total.toFixed(1) + ' TB');

        var arc = d3.select('#graph')
            .attr('viewBox', '-35 -35 70 100')
            .selectAll('.velem')
            .data(pie(graphData))
            .enter()
            .append('g').classed('velem', true);

        arc.append('path')
            .attr('d', arcGen)
            .attr('fill', function (d, i) { return colors[i]; })
            .attr('stroke', 'white')
            .attr('stroke-width', function (d) { if (d.data.size < total * 0.01) return 0; else return 0.1; });

        arc.each(function (d) {
            if (d.data.size > total * 0.02) {
                d3.select(this).append('text')
                    .attr('font-size', 2)
                    .attr('transform', 'translate(' + labelArcGen.centroid(d) + ')')
                    .attr('text-anchor', 'middle')
                    .text((d.data.size / total * 100).toFixed(1) + '%');
            }
        });

        var legend = d3.select('#legend')
            .attr('height', 10 + 20 * graphData.length) // 20 px per row
            .attr('viewBox', '0 0 ' + legendWidth + ' ' + (1 + 2 * graphData.length)); // svg coordinate is 1/10 of browser

        var entries = legend.selectAll('.legendEntry')
            .data(graphData)
            .enter().append('g').classed('legendEntry', true)
            .attr('transform', function (d, i) { return 'translate(1,' + (1 + 2 * i) + ')'; });

        entries.append('circle')
            .attr('cx', 1.)
            .attr('cy', 1.)
            .attr('r', 0.9)
            .attr('fill', function (d, i) { return colors[i]; });

        entries.append('text')
            .attr('font-size', 1.5)
            .attr('dx', 2.5)
            .attr('dy', 1.5)
            .text(function (d) { return d.key; })
            .each(function () { truncateText(this, legendWidth - 3); } );
    }
    else if (data.dataType == 'replication') {
        // data.content: [{key: (key_name), mean: (mean), rms: (rms)}]

        d3.select('#axisBox').style('height', '3%');
        d3.select('#graphBox').style('height', '97%');

        var graphArea = d3.select('#graph')
            .attr('viewBox', '0 0 70 ' + (40 + 4 * data.content.length));

        var axisArea = d3.select('#axis')
            .attr('viewBox', '0 0 70 3');

        var x = d3.scale.linear()
            .domain([0, d3.max(data.content, function (d) { return d.mean + d.rms + 0.5; })])
            .range([0, 50]);

        var xoffset = 20;

        var xaxis = d3.svg.axis()
            .scale(x)
            .orient('top')
            .tickSize(1, 0);

        var gxaxis = axisArea.append('g').classed('axis', true)
            .attr('transform', 'translate(' + xoffset + ',2.5)')
            .call(xaxis);

        gxaxis.selectAll('.tick text')
            .attr('y', -1.2)
            .attr('font-size', 1);

        gxaxis.selectAll('.tick line')
            .attr('stroke', 'black')
            .attr('stroke-width', 0.1);

        gxaxis.select('path.domain')
            .attr('fill', 'none')
            .attr('stroke', 'black')
            .attr('stroke-width', 0.1);

        var entry = graphArea.selectAll('.velem')
            .data(data.content)
            .enter()
            .append('g').classed('velem', true)
            .attr('transform', function (d, i) { return 'translate(0,' + (i * 3.5 + 0.5) + ')'; });

        entry.append('text')
            .attr('font-size', 1.5)
            .attr('text-anchor', 'end')
            .attr('x', xoffset - 0.5)
            .attr('y', 0)
            .attr('dy', 2)
            .text(function (d) { return d.key; })
            .each(function () { truncateText(this, xoffset); });

        var bar = entry.append('g').classed('bar', true)
            .attr('transform', 'translate(' + xoffset + ',0)');

        bar.append('rect').classed('mean', true)
            .attr('width', function (d, i) { return x(d.mean); })
            .attr('height', 3);

        bar.append('rect').classed('rms', true)
            .attr('transform', function (d, i) { return 'translate(' + (x(Math.max(0., d.mean - d.rms))) + ',1)'; })
            .attr('width', function (d, i) { return x(Math.min(d.mean + d.rms, 2. * d.rms)); })
            .attr('height', 1);

        var legend = d3.select('#legend')
            .attr('height', 400) // 20 px per row
            .attr('viewBox', '0 0 ' + legendWidth + ' 40'); // svg coordinate is 1/10 of browser

        var legendMean = legend.append('g').classed('legendEntry', true)
            .attr('transform', 'translate(1,2)');
        var legendRMS = legend.append('g').classed('legendEntry', true)
            .attr('transform', 'translate(1,4)');

        legendMean.append('rect').classed('mean', true)
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 1.8)
            .attr('height', 1.8);

        legendMean.append('text')
            .attr('font-size', 2)
            .attr('dx', 4)
            .attr('dy', 1.6)
            .text('Mean');

        legendRMS.append('rect').classed('rms', true)
            .attr('x', 0.2)
            .attr('y', 0.2)
            .attr('width', 1.4)
            .attr('height', 1.4);

        legendRMS.append('text')
            .attr('font-size', 2)
            .attr('dx', 4)
            .attr('dy', 1.6)
            .text('RMS');
    }
    else if (data.dataType == 'usage') {
        // data.content: [{site: (site_name), usage: [{key: (key_name), size: (size)}]}]
        // data.keys: [(key_name)]

        d3.select('#axisBox').style('height', '3%');
        d3.select('#graphBox').style('height', '97%');

        var keys = data.keys.slice(0, colors.length - 1);
        keys[keys.length] = 'Others';

        var colorMap = d3.scale.ordinal()
            .domain(keys)
            .range(colors);

        var graphArea = d3.select('#graph')
            .attr('viewBox', '0 0 70 ' + (40 + 4 * data.content.length));

        var axisArea = d3.select('#axis')
            .attr('viewBox', '0 0 70 3');

        var x = d3.scale.linear()
            .domain([0, d3.max(data.content, function (d) { return d3.sum(d.usage, function (u) { return u.size; }); }) + 4]) // +4 for size text
            .range([0, 50]);

        var xoffset = 15;

        var xaxis = d3.svg.axis()
            .scale(x)
            .orient('top')
            .tickSize(1, 0);

        var gxaxis = axisArea.append('g').classed('axis', true)
            .attr('transform', 'translate(' + xoffset + ',2.5)')
            .call(xaxis);

        gxaxis.selectAll('.tick text')
            .attr('y', -1.2)
            .attr('font-size', 1);
        
        gxaxis.selectAll('.tick line')
            .attr('stroke', 'black')
            .attr('stroke-width', 0.1);

        gxaxis.select('path.domain')
            .attr('fill', 'none')
            .attr('stroke', 'black')
            .attr('stroke-width', 0.1);

        gxaxis.append('text').classed('unit', true)
            .attr('x', 51)
            .attr('y', -1)
            .attr('font-size', 1.5)
            .text('TB');

        var entry = graphArea.selectAll('.velem')
            .data(data.content)
            .enter()
            .append('g').classed('velem', true)
            .attr('transform', function (d, i) { return 'translate(0,' + (i * 3.5 + 0.5) + ')'; });

        entry.append('text')
            .attr('font-size', 1.5)
            .attr('text-anchor', 'end')
            .attr('x', xoffset - 0.5)
            .attr('y', 0)
            .attr('dy', 2)
            .text(function (d) { return d.site; })
            .each(function () { truncateText(this, xoffset); });

        entry.append('g')
            .attr('transform', 'translate(' + xoffset + ',0)')
            .each(function (siteData) {
                d3.select(this).selectAll('.usage')
                    .data(siteData.usage)
                    .enter().append('rect').classed('usage', true)
                    .attr('width', function (d) { return x(d.size); })
                    .attr('height', 3)
                    .attr('transform',
                          function (d, i) {
                              return 'translate(' + x(d3.sum(siteData.usage.slice(0, i),
                                                             function (u) { return u.size; }
                                                             )) + ',0)';
                          })
                    .attr('fill', function (d) {
                            var color = colorMap(d.key);
                            if (!color)
                                color = colorMap('Others');
                            return color; });

                var total = 0;
                for (var i in siteData.usage)
                    total += siteData.usage[i].size;

                d3.select(this).append('text')
                    .attr('font-size', 1)
                    .attr('y', 1.7)
                    .text(total.toFixed(1))
                    .attr({'text-anchor': 'start', 'x': x(total) + 0.4});
                });

        var legend = d3.select('#legend')
            .attr('height', 10 + 20 * keys.length) // 20 px per row
            .attr('viewBox', '0 0 ' + legendWidth + ' ' + (1 + 2 * keys.length)); // svg coordinate is 1/10 of browser

        var entries = legend.selectAll('.legendEntry')
            .data(keys)
            .enter().append('g').classed('legendEntry', true)
            .attr('transform', function (d, i) { return 'translate(1,' + (1 + 2 * i) + ')'; });

        entries.append('circle')
            .attr('cx', 1.)
            .attr('cy', 1.)
            .attr('r', 0.9)
            .attr('fill', function (k) { return colorMap(k); });

        entries.append('text')
            .attr('font-size', 2)
            .attr('dx', 4)
            .attr('dy', 1.6)
            .text(function (k) { return k; })
            .each(function () { truncateText(this, legendWidth - 4); } );
    }
}

function changeDataType() {

}

function loadData() {
    var inputData = {
        'getData': 1,
        'dataType': $('#dataType').val(),
        'categories': $('#categories').val(),
        'physical': $('.physical:checked').val(),
        'campaign': $('#campaign').val(),
        'dataTier': $('#dataTier').val(),
        'dataset': $('#dataset').val(),
        'site': $('#site').val(),
        'group': []
    };

    var groups = $('#group :selected').get();
    for (var g in groups)
        inputData.group.push(groups[g].value);

    $.get('inventory.php', inputData, function (data, textStatus, jqXHR) {
            displayData(data);
        }, 'json');
}

function getData() {
    var url = 'inventory.php?';
    url += 'getData=1';
    url += '&dataType=' + $('#dataType').val();
    url += '&categories=' + $('#categories').val();
    url += 'physical=' + $('.physical:checked').val();
    var fields = ['campaign', 'dataTier', 'dataset', 'site'];
    for (var iF in fields) {
        var elem = $('#' + fields[iF]);
        if (elem.val() != '')
            url += '&' + fields[iF] + '=' + elem.val();
    }
    var groups = $('#group :selected').get();
    if (groups.length != 0) {
        url += '&group=';
        for (var g in groups) {
            url += groups[g].value;
            if (g != groups.length - 1)
                url += ',';
        }
    }
    
    window.location = url;
}
