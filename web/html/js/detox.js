var currentRun = 0;
var currentPartition = 0;
var currentNorm = 'relative';

function initPage(runNumber, partitionId)
{
    $.ajax({url: 'detox.php', data: {getPartitions: 1}, success: function (data, textStatus, jqXHR) { setPartitions(data); }, dataType: 'json', async: false});
    
    loadSummary(runNumber, partitionId, currentNorm);
}

function setPartitions(data)
{
    var partitionsNav = d3.select('#partitions');
    partitionsNav.selectAll('.partitionTab')
        .data(data)
        .enter().append('div').classed('partitionTab', true)
        .text(function (d) { return d.name; })
        .attr('id', function (d) { return 'partition' + d.id; })
        .on('click', function (d) { loadSummary(currentRun, d.id, currentNorm); });

    partitionsNav.select(':last-child').classed('last', true);
}

function sortTableBy(siteName, column)
{
    var siteData = {name: siteName, datasets: []};
    var table = d3.select('#' + siteName + ' .siteTable');

    table.selectAll('tr')
        .each(function (d, i) { 
                siteData.datasets[i] = {
                    name: this.childNodes[0].textContent,
                    size: Number(this.childNodes[1].textContent),
                    decision: this.childNodes[2].textContent,
                    reason: this.childNodes[3].textContent
                };
            });

    table.selectAll('tr').remove();

    if (column == 'dataset')
        siteData.datasets.sort(function (r1, r2) { return r1.name.localeCompare(r2.name); });
    else if (column == 'size')
        siteData.datasets.sort(function (r1, r2) { return r1.size - r2.size; });
    else if (column == 'decision')
        siteData.datasets.sort(function (r1, r2) { 
                var diff = r1.decision.localeCompare(r2.decision);
                return diff;
            });

    displayDetails(siteData);
}

function displaySummary(data)
{
    d3.select('#runNumber').text(data.runNumber);
    d3.select('#runTimestamp').text(data.runTimestamp);

    if (data.siteData.length == 0)
        return;

    // draw summary graph
    var summaryGraph = d3.select('#summaryGraphBox').append('svg')
        .attr('id', 'summaryGraph');

    summaryGraph.selectAll('*').remove();

    summaryGraph
        .attr('viewBox', '0 0 500 200')
        .style({'width': '100%', 'height': '600px'});

    var xorigin = 20;
    var yorigin = 145;
    var xmax = 500 - xorigin;
    var ymarginTop = 15;
    var ymarginBottom = 200 - yorigin;
    var ymax = yorigin - ymarginTop;

    var xmapping = d3.scale.ordinal()
        .domain(data.siteData.map(function (v) { return v.name; }))
        .rangePoints([0, xmax], 1);

    var xspace = xmax / data.siteData.length;

    var yscale = d3.scale.linear();

    var ynorm;

    var titleRelative = summaryGraph.append('text')
        .attr('font-size', 8)
        .attr('x', xorigin + 2)
        .attr('y', -2)
        .text('Normalized site usage');

    var selectRelative = summaryGraph.append('circle')
        .attr('cx', xorigin - 3)
        .attr('cy', -5)
        .attr('r', 3)
        .attr('stroke', 'black')
        .attr('stroke-width', 0.3)
        .attr('fill', 'none');

    var titleAbsolute = summaryGraph.append('text')
        .attr('font-size', 8)
        .attr('x', xorigin + 2)
        .attr('y', 8)
        .text('Absolute data volume');

    var selectAbsolute = summaryGraph.append('circle')
        .attr('cx', xorigin - 3)
        .attr('cy', 5)
        .attr('r', 3)
        .attr('stroke', 'black')
        .attr('stroke-width', 0.3)
        .attr('fill', 'none');

    var eye = summaryGraph.append('circle')
        .attr('fill', 'black')
        .attr('r', 2);

    if (currentNorm == 'relative') {
        eye.attr('cx', xorigin - 3)
            .attr('cy', -5);

        titleAbsolute.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentRun, currentPartition, \'absolute\');');

        yscale.domain([0, 1.25])
            .range([ymax, 0]);

        ynorm = function (d, key) { if (d.quota == 0.) return 0.; else return ymax - yscale(d[key] / d.quota); };
    }
    else {
        eye.attr('cx', xorigin - 3)
            .attr('cy', 5);

        titleRelative.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentRun, currentPartition, \'relative\');');

        yscale.domain([0, d3.max(data.siteData, function (d) { return Math.max(d.protect + d.keep + d.delete, d.protectPrev + d.keepPrev); }) * 1.25])
            .range([ymax, 0]);

        ynorm = function (d, key) { return ymax - yscale(d[key]); };
    }

    var xaxis = d3.svg.axis()
        .scale(xmapping)
        .orient('bottom')
        .tickSize(0, 0);

    var yaxis = d3.svg.axis()
        .scale(yscale)
        .orient('left')
        .tickSize(1, 0);

    var gxaxis = summaryGraph.append('g').classed('axis', true)
        .attr('transform', 'translate(' + xorigin + ',' + yorigin + ')')
        .call(xaxis);

    gxaxis.selectAll('.tick text')
        .attr('font-size', 5.5)
        .attr('dx', -1)
        .attr('dy', -1)
        .attr('transform', 'rotate(300 0,0)')
        .style('text-anchor', 'end');

    gxaxis.selectAll('.tick')
        .append('a')
        .attr('xlink:href', function () { return '#' + d3.select(this.parentNode).select('text').text(); })
        .append('rect')
        .attr('transform', 'rotate(120 0,0) translate(0,-' + (xspace * 0.5) + ')')
        .attr('fill', 'white')
        .attr('fill-opacity', 0)
        .attr('width', ymarginBottom)
        .attr('height', xspace);

    gxaxis.select('path.domain')
        .attr('fill', 'none')
        .attr('stroke', 'black')
        .attr('stroke-width', 0.2);

    var gyaxis = summaryGraph.append('g').classed('axis', true)
        .attr('transform', 'translate(' + xorigin + ',' + ymarginTop + ')')
        .call(yaxis);

    gyaxis.selectAll('.tick text')
        .attr('font-size', 5);

    gyaxis.selectAll('.tick line')
        .attr('stroke', 'black')
        .attr('stroke-width', 0.2);

    gyaxis.select('path.domain')
        .attr('fill', 'none')
        .attr('stroke', 'black')
        .attr('stroke-width', 0.2);

    var content = summaryGraph.append('g').classed('content', true)
        .attr('transform', 'translate(' + xorigin + ',' + yorigin + ')');

    if (currentNorm == 'relative') {
        content.append('line').classed('refMarker', true)
            .attr('x1', 0)
            .attr('x2', xmax)
            .attr('y1', -ymax / 1.25)
            .attr('y2', -ymax / 1.25);

        content.append('line').classed('refMarker', true)
            .attr('x1', 0)
            .attr('x2', xmax)
            .attr('y1', -ymax * 0.5 / 1.25)
            .attr('y2', -ymax * 0.5 / 1.25)
            .attr('stroke-dasharray', '3,3');

        content.append('line').classed('refMarker', true)
            .attr('x1', 0)
            .attr('x2', xmax)
            .attr('y1', -ymax * 0.9 / 1.25)
            .attr('y2', -ymax * 0.9 / 1.25)
            .attr('stroke-dasharray', '1,1');

        content.selectAll('.refMarker')
            .attr('stroke', 'black')
            .attr('stroke-width', 0.2);
    }
    else {
        gyaxis.append('text')
            .attr('transform', 'translate(-10,0)')
            .attr('font-size', 5)
            .text('TB');
    }

    var barPrev = content.selectAll('.barPrev')
        .data(data.siteData)
        .enter()
        .append('g').classed('barPrev', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) - xspace * 0.325) + ',0)'; });

    var y = ynorm(data.siteData[0], 'protect');

    barPrev.append('rect').classed('protectPrev barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + ynorm(d, 'protectPrev') + ')'; })
        .attr('height', function (d) { return ynorm(d, 'protectPrev')});

    barPrev.append('rect').classed('keepPrev barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d, 'protectPrev') + ynorm(d, 'keepPrev')) + ')'; })
        .attr('height', function (d) { return ynorm(d, 'keepPrev')});

    var barNew = content.selectAll('.barNew')
        .data(data.siteData)
        .enter().append('g').classed('barNew', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) + xspace * 0.025) + ',0)'; });

    barNew.append('rect').classed('protect barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + ynorm(d, 'protect') + ')'; })
        .attr('height', function (d) { return ynorm(d, 'protect')});

    barNew.append('rect').classed('keep barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d, 'protect') + ynorm(d, 'keep')) + ')'; })
        .attr('height', function (d) { return ynorm(d, 'keep')});

    barNew.append('rect').classed('delete barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d, 'protect') + ynorm(d, 'keep') + ynorm(d, 'delete')) + ')'; })
        .attr('height', function (d) { return ynorm(d, 'delete')});

    content.selectAll('.barComponent')
        .attr('width', xspace * 0.3);

    var legend = summaryGraph.append('g').classed('legend', true)
        .attr('transform', 'translate(360, 5)');

    var legendContents =
        [{'cls': 'keepPrev', 'title': 'Kept in previous run', 'position': '(0,10)'},
         {'cls': 'protectPrev', 'title': 'Protected in previous run', 'position': '(0,20)'},
         {'cls': 'delete', 'title': 'Deleted', 'position': '(80,0)'},
         {'cls': 'keep', 'title': 'Kept', 'position': '(80,10)'},
         {'cls': 'protect', 'title': 'Protected', 'position': '(80,20)'}];

    var legendEntries = legend.selectAll('g')
        .data(legendContents)
        .enter()
        .append('g')
        .attr('transform', function (d) { return 'translate' + d.position; });

    legendEntries.append('circle')
        .attr('cx', 5)
        .attr('cy', 5)
        .attr('r', 4)
        .attr('class', function (d) { return d.cls; });

    legendEntries.append('text')
        .attr('font-size', 5)
        .attr('dx', 12)
        .attr('dy', 7)
        .text(function (d) { return d.title; });

    // set up tables for individual sites

    d3.select('#details').selectAll('.siteDetails').remove();
    
    var siteDetails = d3.select('#details').selectAll('.siteDetails')
        .data(data.siteData)
        .enter()
        .append('article').classed('siteDetails', true)
        .attr('id', function (d) { return d.name; });

    // everything that is selected from siteDetails will have data = site data

    siteDetails.append('h3').classed('siteName', true)
        .text(function (d) { return d.name; });

    var tableBox = siteDetails.append('div').classed('siteTableBox', true);

    var table = tableBox.append('table').classed('siteTable', true);

    var tableNode = table.node();

    var headerRow = table.append('thead').append('tr');

    headerRow.append('th').classed('datasetCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.65 - 1) + 'px')
        .text('Dataset')
        .on('click', function (d) { sortTableBy(d.name, 'dataset'); });

    headerRow.append('th').classed('sizeCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text('Size (GB)')
        .on('click', function (d) { sortTableBy(d.name, 'size'); });

    headerRow.append('th').classed('decisionCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text('Decision')
        .on('click', function (d) { sortTableBy(d.name, 'decision'); });

    headerRow.append('th').classed('reasonCol', true)
        .style('width', (tableNode.clientWidth * 0.25) + 'px')
        .text('Reason');

    d3.select(window).on('resize', padTables);

    tableBox.append('div')
        .style({'width': '20%', 'height': '10%', 'margin': '20% 40% 0 40%', 'cursor': 'pointer', 'text-decoration': 'underline', 'text-align': 'center'})
        .text('Load site data')
        .on('click', function (d) { d3.select(this).remove(); loadSiteTable(d.name); });
}

function padTables()
{
    var table = d3.select('table.siteTable');
    var tableNode = table.node();
    table.select('th.datasetCol').style('width', (tableNode.clientWidth * 0.65 - 1) + 'px');
    table.select('tr.datasetCol').style('width', (tableNode.clientWidth * 0.65 - 1) + 'px');
    table.select('th.sizeCol').style('width', (tableNode.clientWidth * 0.05 - 1) + 'px');
    table.select('tr.sizeCol').style('width', (tableNode.clientWidth * 0.05 - 1) + 'px');
    table.select('th.decisionCol').style('width', (tableNode.clientWidth * 0.05 - 1) + 'px');
    table.select('tr.decisionCol').style('width', (tableNode.clientWidth * 0.05 - 1) + 'px');
    table.select('th.reasonCol').style('width', (tableNode.clientWidth * 0.25) + 'px');
    table.select('tr.reasonCol').style('width', (tableNode.clientWidth * 0.25) + 'px');
}

function displayDetails(siteData)
{
    var block = d3.select('#' + siteData.name);
    
    if (siteData.datasets.length == 0) {
        block.select('.siteTable').remove();
        block.select('.siteTableBox')
            .style({'font-size': '108px;', 'text-align': 'center', 'padding-top': '150px', 'font-weight': '500'})
            .text('Empty');

        return;
    }
    
    var table = block.select('.siteTable');
    var tableNode = table.node();

    var tbody = table.append('tbody')
        .style({'height': '656px', 'overflow-y': 'auto', 'overflow-x': 'hidden'});

    var row = tbody.selectAll('tr')
        .data(siteData.datasets)
        .enter()
        .append('tr')
        .each(function (d, i) { if (i % 2 == 1) d3.select(this).classed('odd', true); });

    row.append('td').classed('datasetCol', true)
        .style({'width': (tableNode.clientWidth * 0.65 - 1) + 'px', 'font-size': '10px'})
        .text(function (d) { return d.name; });
    row.append('td').classed('sizeCol', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text(function (d) { return d.size.toFixed(1); });
    row.append('td').classed('decisionCol', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text(function (d) { return d.decision; });
    row.append('td').classed('reasonCol', true)
        //        .style('width', (tableNode.clientWidth * 0.25) + 'px')
        .text(function (d) { return d.reason; });
}

function loadSummary(runNumber, partitionId, summaryNorm)
{
    currentRun = runNumber;
    currentPartition = partitionId;
    currentNorm = summaryNorm;

    d3.selectAll('.partitionTab')
        .classed('selected', false);
    
    d3.select('#partition' + partitionId)
        .classed('selected', true);

    var box = d3.select('#summaryGraphBox');
    box.selectAll('*').remove();

    var spinner = new Spinner({scale: 5, corners: 0, width: 2, position: 'absolute'});
    spinner.spin();
    $(box.node()).append($(spinner.el));

    var inputData = {
        getData: 1,
        dataType: 'summary',
        runNumber: runNumber,
        partitionId: partitionId,
    };

    $.ajax({url: 'detox.php', data: inputData, success: function (data, textStatus, jqXHR) {
                displaySummary(data);
                spinner.stop();
            }, dataType: 'json', async: false});
}

function loadSiteTable(name)
{
    var spinner = new Spinner({scale: 5, corners: 0, width: 2, position: 'relative'});
    spinner.spin();
    $('#' + name + ' .siteTableBox').append($(spinner.el));

    var inputData = {
        getData: 1,
        dataType: 'siteDetail',
        runNumber: currentRun,
        partitionId: currentPartition,
        siteName: name
    };

    $.get('detox.php', inputData, function (data, textStatus, jqXHR) {
            displayDetails(data);
            spinner.stop();
    }, 'json');
}
