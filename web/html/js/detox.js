var currentCycle = 0;
var nextCycle = 0;
var previousCycle = 0;
var latestCycle = 0;
var currentPartition = 0;
var currentNorm = 'relative';

var summary = {
    'bars': null,
    'yscale': null,
    'xorigin': 6,
    'yorigin': 74,
    'xmax': 100 - 6,
    'ymax': 62
};

var siteDetails;

function initPage(cycleNumber, partitionId)
{
    var jaxData = {
        'url': window.location.href,
        'data': {'getPartitions': 1},
        'success': function (data, textStatus, jqXHR) { setPartitions(data); },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
    
    loadSummary(cycleNumber, partitionId, currentNorm);

    if (window.location.href.indexOf('cycleNumber') < 0)
        latestCycle = currentCycle;

    setInterval(checkUpdates, 300000);
}

function checkUpdates()
{
    if (window.location.href.indexOf('cycleNumber') > 0 || currentCycle != latestCycle)
        return;

    var jaxData = {
        'url': window.location.href,
        'data': {'checkUpdate': 1, 'partitionId': currentPartition},
        'success': function (latest, textStatus, jqXHR) { processUpdates(latest); },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
}

function setPartitions(data)
{
    var partitionsNav = d3.select('#partitions');
    partitionsNav.selectAll('.partitionTab')
        .data(data)
        .enter().append('div').classed('partitionTab', true)
        .text(function (d) { return d.name; })
        .attr('id', function (d) { return 'partition' + d.id; })
        .on('click', function (d) { window.location.assign(window.location.protocol + '//' + window.location.hostname + window.location.pathname + '?partitionId=' + d.id); });

    partitionsNav.select(':last-child').classed('last', true);
}

function processUpdates(latest)
{
    if (latest == latestCycle)
        return;

    latestCycle = latest;
    d3.select('#cycleHeader').append('div')
        .text('New cycle ' + latest + ' is available')
        .style({'cursor': 'pointer', 'font-size': '18px'})
        .on('click', function () { window.location.assign(window.location.protocol + '//' + window.location.hostname + window.location.pathname + '?partitionId=' + currentPartition); });
}

function setupSiteDetails(siteData)
{
    // set up tables for individual sites

    d3.select('#details').selectAll('.siteDetails').remove();

    // global variable
    siteDetails = d3.select('#details').selectAll('.siteDetails')
        .data(siteData)
        .enter()
        .append('article').classed('siteDetails', true)
        .attr('id', function (d) { return d.name; });

    // everything that is selected from siteDetails will have data = site data

    siteDetails.append('h3').classed('siteName', true)
        .text(function (d) {
                var text = d.name + ' (' +
                    d.delete.toFixed(1) + ' TB Delete, ' +
                    d.keep.toFixed(1) + ' TB Keep, ' +
                    d.protect.toFixed(1) + ' TB Protect, ' +
                    (d.delete + d.keep + d.protect).toFixed(1) + ' TB Total / ' +
                    'Quota ' + d.quota + ' TB)';
                return text;
            });

    siteDetails.append('div').classed('toTop', true)
        .html('&#9650; Back to top')
        .on('click', function () { window.scrollTo(0, 0); });

    var tableBox = siteDetails.append('div').classed('siteTableBox', true)
        .style({'height': '82px', 'position': 'relative', 'top': 0, 'left': 0}); // needed to place objects in the box

    var table = tableBox.append('table').classed('siteTable', true);

    var tableNode = table.node();

    var headerRow = table.append('thead').append('tr');

    headerRow.append('th').classed('datasetCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.65 - 1) + 'px')
        .text('Dataset')
        .on('click', function (d) { sortTableBy(d.name, 'datasetCol', 1); });

    headerRow.append('th').classed('sizeCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text('Size (GB)')
        .on('click', function (d) { sortTableBy(d.name, 'sizeCol', 1); });

    headerRow.append('th').classed('decisionCol sortable', true)
        .style('width', (tableNode.clientWidth * 0.05 - 1) + 'px')
        .text('Decision')
        .on('click', function (d) { sortTableBy(d.name, 'decisionCol', 1); });

    headerRow.append('th').classed('reasonCol', true)
        .style('width', (tableNode.clientWidth * 0.25) + 'px')
        .text('Reason');

    d3.select(window).on('resize', padTables);

    tableBox.append('div').classed('loadSiteData', true)
        .style({'width': '20%', 'margin': '10px 40% 0 40%', 'cursor': 'pointer', 'text-decoration': 'underline', 'text-align': 'center'})
        .text('Load site data')
        .on('click', function (d) { d3.select(this).remove(); loadSiteTable(d.name); });
}

function sortTableBy(siteName, column, direction)
{
    var siteData = {'name': siteName, 'datasets': []};
    var tableBox = d3.select('#' + siteName + ' .siteTableBox');
    var table = tableBox.select('.siteTable');
    var tbody = table.select('tbody');

    var mask = tableBox.append('div')
        .style({'width': '100%', 'height': '100%', 'position': 'absolute', 'top': 0, 'background-color': 'white', 'opacity': 0.5});

    tbody.remove();

    tbody.selectAll('tr')
        .each(function (d, i) {
                siteData.datasets[i] = {
                    'name': this.childNodes[0].textContent,
                    'size': Number(this.childNodes[1].textContent),
                    'decision': this.childNodes[2].textContent,
                    'reason': this.childNodes[3].textContent
                };
            });

    var headText;
    if (column == 'datasetCol') {
        siteData.datasets.sort(function (r1, r2) { return direction * r1.name.localeCompare(r2.name); });
        headText = 'Dataset';
    }
    else if (column == 'sizeCol') {
        siteData.datasets.sort(function (r1, r2) { return direction * (r1.size - r2.size); });
        headText = 'Size (GB)';
    }
    else if (column == 'decisionCol') {
        siteData.datasets.sort(function (r1, r2) { return direction * r1.decision.localeCompare(r2.decision); });
        headText = 'Decision';
    }

    if (direction > 0)
        headText += '&#9660;'; // filled down-pointing triangle
    else
        headText += '&#9650;'; // filled up-pointing triangle

    window.setTimeout(function () {
            displayDetails(siteData);
            mask.remove();
        }, 100);

    table.select('th.' + column)
        .html(headText)
        .on('click', function (d) { sortTableBy(d.name, column, -direction); });
}

function displaySummary(data)
{
    var cycleHeader = d3.select('#cycleHeader');
    cycleHeader.append('span').text('Cycle ' + data.cycleNumber + ' (Policy ');
    if (data.cyclePolicy != '') {
        cycleHeader.append('span').text(data.cyclePolicy)
            .classed('clickable', true)
            .on('click', function () { window.open('https://github.com/SmartDataProjects/dynamo-policies/tree/' + this.textContent); });
    }
    else {
        cycleHeader.append('span').text('unknown');
    }
    cycleHeader.append('span').text(', ' + data.cycleTimestamp + ')');

    if (data.siteData.length == 0)
        return;

    // draw summary graph
    var box = d3.select('#summaryGraphBox');
    var boxNode = box.node();
    var summaryGraph = box.append('svg')
        .attr('id', 'summaryGraph');

    summaryGraph.selectAll('*').remove();

    // scale 0-100 to actual x according to aspect ratio
    var gxscale = boxNode.clientWidth / boxNode.clientHeight;

    var gxnorm = function (x) { return x * gxscale; }

    summaryGraph
        .attr('viewBox', '0 0 ' + (100 * gxscale) + ' 100')
        .style({'width': '100%', 'height': '100%'});

    var xmapping = d3.scale.ordinal()
        .domain(data.siteData.map(function (v) { return v.name; }))
        .rangePoints([0, gxnorm(summary.xmax)], 1);

    var xspace = gxnorm(summary.xmax / data.siteData.length);

    // global variable
    summary.yscale = d3.scale.linear();

    var ynorm;

    var titleRelative = summaryGraph.append('text')
        .attr({'font-size': 3, 'x': gxnorm(summary.xorigin + 3), 'y': 4})
        .text('Normalized site usage');

    var selectRelative = summaryGraph.append('circle')
        .attr({'cx': gxnorm(summary.xorigin + 2), 'cy': 3, 'r': 1, 'stroke': 'black', 'stroke-width': 0.3, 'fill': 'white'});

    var titleAbsolute = summaryGraph.append('text')
        .attr({'font-size': 3, 'x': gxnorm(summary.xorigin + 3), 'y': 8})
        .text('Absolute data volume');

    var selectAbsolute = summaryGraph.append('circle')
        .attr({'cx': gxnorm(summary.xorigin + 2), 'cy': 7, 'r': 1, 'stroke': 'black', 'stroke-width': 0.3, 'fill': 'white'});

    var eye = summaryGraph.append('circle')
        .attr({'fill': 'black', 'cx': gxnorm(summary.xorigin + 2), 'r': 0.6});

    if (currentNorm == 'relative') {
        eye.attr('cy', 3);

        selectAbsolute.attr('onclick', 'loadSummary(currentCycle, currentPartition, \'absolute\');');

        titleAbsolute.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentCycle, currentPartition, \'absolute\');');

        summary.yscale.domain([0, 1.25])
            .range([summary.ymax, 0]);

        ynorm = function (value, quota) { if (quota == 0.) return 0.; else return summary.ymax - summary.yscale(value / quota); }
    }
    else {
        eye.attr('cy', 7);

        selectRelative.attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');

        titleRelative.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');

        summary.yscale.domain([0, d3.max(data.siteData, function (d) { return Math.max(d.protect + d.keep + d.delete, d.protectPrev + d.keepPrev, d.quota) / 1000.; }) * 1.1])
            .range([summary.ymax, 0]);

        ynorm = function (value, quota) { return (summary.ymax - summary.yscale(value)) / 1000.; }
    }

    var xaxis = d3.svg.axis()
        .scale(xmapping)
        .orient('bottom')
        .tickSize(0, 0);

    var yaxis = d3.svg.axis()
        .scale(summary.yscale)
        .orient('left')
        .tickSize(1, 0);

    var gxaxis = summaryGraph.append('g').classed('axis', true)
        .attr('transform', 'translate(' + gxnorm(summary.xorigin) + ',' + summary.yorigin + ')')
        .call(xaxis);

    var siteStatus = {};
    for (var s in data.siteData)
        siteStatus[data.siteData[s].name] = data.siteData[s].status;

    gxaxis.selectAll('.tick text')
        .attr({'font-size': 2, 'dx': gxnorm(-0.2), 'dy': -1.4, 'transform': 'rotate(300 0,0)'})
        .attr('onclick', function (siteName) { return 'd3.select(\'#' + siteName + '\').node().scrollIntoView();'; })
        .style({'text-anchor': 'end', 'cursor': 'pointer'})
        .each(function (siteName) {
                if (siteStatus[siteName] == 0)
                    d3.select(this).attr('fill', '#808080');
            });

    gxaxis.select('path.domain')
        .attr({'fill': 'none', 'stroke': 'black', 'stroke-width': 0.2});

    var gyaxis = summaryGraph.append('g').classed('axis', true)
        .attr('transform', 'translate(' + gxnorm(summary.xorigin) + ',' + (summary.yorigin - summary.ymax) + ')')
        .call(yaxis);

    gyaxis.selectAll('.tick text')
        .attr('font-size', 3);

    gyaxis.selectAll('.tick line')
        .attr({'stroke': 'black', 'stroke-width': 0.2});

    gyaxis.select('path.domain')
        .attr({'fill': 'none', 'stroke': 'black', 'stroke-width': 0.2});

    var content = summaryGraph.append('g').classed('content', true)
        .attr('transform', 'translate(' + gxnorm(summary.xorigin) + ',' + summary.yorigin + ')');

    var gridLine = content.selectAll('.gridLine')
        .data(summary.yscale.ticks())
        .enter()
        .append('line').classed('gridLine', true)
        .attr({'x1': 0, 'x2': gxnorm(summary.xmax), 'stroke-dasharray': '0.2,0.2', 'stroke-width': 0.2, 'stroke': 'silver'});

    if (currentNorm == 'relative') {
        gridLine
            .attr('y1', function (d) { return -ynorm(d, 1.0); })
            .attr('y2', function (d) { return -ynorm(d, 1.0); });

        var refMarkers = content.selectAll('.refMarker')
            .data([1.0, 0.9, 0.85])
            .enter()
            .append('line').classed('refMarker', true)
            .attr({'x1': 0, 'x2': gxnorm(summary.xmax)})
            .attr('y1', function (d) { return -ynorm(d, 1.0); })
            .attr('y2', function (d) { return -ynorm(d, 1.0); });

        d3.select(refMarkers[0][0]).classed('quota', true);
        d3.select(refMarkers[0][1]).classed('trigger', true);
        d3.select(refMarkers[0][2]).classed('target', true);
    }
    else {
        gyaxis.append('text')
            .attr({'transform': 'translate(' + gxnorm(-2) + ',-2)', 'font-size': 3})
            .text('PB');

        gridLine
            .attr('y1', function (d) { return -ynorm(d * 1000., 1.0); })
            .attr('y2', function (d) { return -ynorm(d * 1000., 1.0); });

        var refMarkers = content.selectAll('.refMarkers')
            .data(data.siteData)
            .enter()
            .append('g').classed('refMarkers', true)
            .attr('transform', function (d) { return 'translate(' + xmapping(d.name) + ',0)'; });

        refMarkers.append('line').classed('refMarker quota', true)
            .attr('y1', function (d) { return -ynorm(d.quota); })
            .attr('y2', function (d) { return -ynorm(d.quota); });
        refMarkers.append('line').classed('refMarker trigger', true)
            .attr('y1', function (d) { return -ynorm(d.quota * 0.9); })
            .attr('y2', function (d) { return -ynorm(d.quota * 0.9); });
        refMarkers.append('line').classed('refMarker target', true)
            .attr('y1', function (d) { return -ynorm(d.quota * 0.85); })
            .attr('y2', function (d) { return -ynorm(d.quota * 0.85); });

        refMarkers.selectAll('.refMarker')
            .attr({'x1': -xspace * 0.5, 'x2': xspace * 0.5});
    }

    var barPrev = content.selectAll('.barPrev')
        .data(data.siteData)
        .enter()
        .append('g').classed('barPrev', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) - xspace * 0.325) + ',0)'; })
        .attr('onclick', function (d) { return 'd3.select(\'#' + d.name + '\').node().scrollIntoView();'; })
        .style('cursor', 'pointer');

    barPrev.append('rect').classed('protectPrev barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + ynorm(d.protectPrev, d.quota) + ')'; })
        .attr('height', function (d) { return ynorm(d.protectPrev, d.quota)})
        .each(function (d) {
                if (d.protectPrev > d.quota)
                    this.style.fill = '#ff8888';
                else if (d.protectPrev > d.quota * 0.9)
                    this.style.fill = '#ffbb88';
            });

    barPrev.append('rect').classed('keepPrev barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d.protectPrev, d.quota) + ynorm(d.keepPrev, d.quota)) + ')'; })
        .attr('height', function (d) { return ynorm(d.keepPrev, d.quota); });

    // global variable
    summary.bars = content.selectAll('.barNew')
        .data(data.siteData)
        .enter().append('g').classed('barNew', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) + xspace * 0.025) + ',0)'; })
        .attr('onclick', function (d) { return 'd3.select(\'#' + d.name + '\').node().scrollIntoView();'; })
        .style('cursor', 'pointer');

    summary.bars.append('rect').classed('protect barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + ynorm(d.protect, d.quota) + ')'; })
        .attr('height', function (d) { return ynorm(d.protect, d.quota); })
        .each(function (d) {
                if (d.protect > d.quota)
                    this.style.fill = '#ff0000';
                else if (d.protectPrev > d.quota * 0.9)
                    this.style.fill = '#ff8800';
            });

    summary.bars.append('rect').classed('keep barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d.protect, d.quota) + ynorm(d.keep, d.quota)) + ')'; })
        .attr('height', function (d) { return ynorm(d.keep, d.quota); });

    summary.bars.append('rect').classed('delete barComponent', true)
        .attr('transform', function (d) { return 'translate(0,-' + (ynorm(d.protect, d.quota) + ynorm(d.keep, d.quota) + ynorm(d.delete, d.quota)) + ')'; })
        .attr('height', function (d) { return ynorm(d.delete, d.quota); });

    content.selectAll('.barComponent')
        .attr('width', xspace * 0.3);

    content.selectAll('.siteMask')
        .data(data.siteData)
        .enter().append('g').classed('siteMask', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) - xspace * 0.325) + ',0)'; })
        .each(function (d) {
                if (d.status == 0) {
                    var mask = d3.select(this);
                    var height = ynorm(d.protect, d.quota) + ynorm(d.keep, d.quota) + ynorm(d.delete, d.quota);
                    mask.append('rect')
                        .attr('transform', 'translate(0,-' + height + ')')
                        .attr({'width': xspace * 0.65, 'height': height, 'fill': 'dimgrey', 'fill-opacity': 0.5});
                }
            });

    var lineLegend = summaryGraph.append('g').classed('lineLegend', true)
        .attr('transform', 'translate(' + gxnorm(56) + ', 0)');

    var lineLegendContents =
        [{'title': 'Deletion trigger', 'stroke': 'darkorange', 'position': '(0,3.5)'},
         {'title': 'Target occupancy', 'stroke': 'silver', 'position': '(0,7.5)'}];

    var lineLegendEntries = lineLegend.selectAll('g')
        .data(lineLegendContents)
        .enter()
        .append('g')
        .attr('transform', function (d) { return 'translate' + d.position });

    lineLegendEntries.append('line')
        .classed('refMarker', true)
        .attr({'x1': 0, 'x2': gxnorm(2), 'y1': 1, 'y2': 1, 'stroke-width': 0.2})
        .attr('stroke', function (d) { return d.stroke; });

    lineLegendEntries.append('text')
        .attr({'font-size': 2, 'dx': gxnorm(3), 'dy': 2})
        .text(function (d) { return d.title; });

    var legend = summaryGraph.append('g').classed('legend', true)
        .attr('transform', 'translate(' + gxnorm(68) + ', 0)');

    var legendContents =
        [{'cls': 'delete', 'title': 'Deleted', 'position': '(0,3.5)'},
         {'cls': 'keep', 'title': 'Kept', 'position': '(0,7.5)'},
         {'cls': 'protect', 'title': 'Protected', 'position': '(0,11.5)'},
         {'cls': 'keepPrev', 'title': 'Kept in previous cycle', 'position': '(' + gxnorm(10) + ',3.5)'},
         {'cls': 'protectPrev', 'title': 'Protected in previous cycle', 'position': '(' + gxnorm(10) + ',7.5)'}];

    var legendEntries = legend.selectAll('g')
        .data(legendContents)
        .enter()
        .append('g')
        .attr('transform', function (d) { return 'translate' + d.position; });

    legendEntries.append('circle')
        .attr({'cx': gxnorm(2), 'cy': 1, 'r': 1.5})
        .attr('class', function (d) { return d.cls; });

    legendEntries.append('text')
        .attr({'font-size': 2, 'dx': gxnorm(3), 'dy': 2})
        .text(function (d) { return d.title; });
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

function addTableRows(table, tbodyClass, data)
{
    var tbody = table.append('tbody').classed(tbodyClass, true)
        .style({'overflow-y': 'auto', 'overflow-x': 'hidden'});

    var row = tbody.selectAll('tr')
        .data(data)
        .enter()
        .append('tr')
        .each(function (d, i) { if (i % 2 == 1) d3.select(this).classed('odd', true); });

    var tableWidth = table.node().clientWidth;

    row.append('td').classed('datasetCol', true)
        .style({'width': (tableWidth * 0.65 - 1) + 'px', 'font-size': '14px'})
        .text(function (d) { return d.name; });
    row.append('td').classed('sizeCol', true)
        .style('width', (tableWidth * 0.05 - 1) + 'px')
        .text(function (d) { return d.size.toFixed(1); });
    row.append('td').classed('decisionCol', true)
        .style('width', (tableWidth * 0.05 - 1) + 'px')
        .text(function (d) { return d.decision; });
    row.append('td').classed('reasonCol', true)
        .text(function (d) { return d.reason; });

    return tbody;
}

function displayDetails(siteData)
{
    var block = d3.select('#' + siteData.name);

    var tableBox = block.select('.siteTableBox');
    var table = block.select('.siteTable');
    
    if (siteData.datasets.length == 0) {
        table.remove();
        tableBox.style({'height': '82px', 'font-size': '108px;', 'text-align': 'center', 'padding-top': '150px', 'font-weight': '500'})
            .text('Empty');

        return;
    }

    tableBox.style('height', '700px');
    
    var tbody = addTableRows(table, 'full', siteData.datasets);
    tbody.style('height', '656px');
}

function displayDatasetSearch(data)
{
    // data: [name: name, datasets: [{name: name, size: size, decision: dec, reason: reason}]]

    // Add bars corresponding to searched datasets to the summary graph
    var ynorm;

    if (currentNorm == 'relative')
        ynorm = function (d, val) { if (d.quota == 0.) return 0.; else return summary.ymax - summary.yscale(val / d.quota); }
    else
        ynorm = function (d, val) { return (summary.ymax - summary.yscale(val)) / 1000.; }

    summary.bars.each(function (d) {
            var x = 0;
            while (x != data.length) {
                if (d.name == data[x].name)
                    break;
                x += 1;
            }

            if (x == data.length)
                return;

            var siteDatasets = data[x].datasets;

            var protectTotal = 0;
            var keepTotal = 0;
            var deleteTotal = 0;
            for (var x in siteDatasets) {
                var dataset = siteDatasets[x];
                if (dataset.decision == 'protect')
                    protectTotal += dataset.size;
                else if (dataset.decision == 'keep')
                    keepTotal += dataset.size;
                else
                    deleteTotal += dataset.size;
            }
            protectTotal /= 1000.;
            keepTotal /= 1000.;
            deleteTotal /= 1000.;

            var bar = d3.select(this);
            var barWidth = bar.select('.barComponent').attr('width');

            if (protectTotal > 0) {
                bar.append('rect')
                    .classed('searched barComponent', true)
                    .attr('transform', function (d) { return 'translate(0,-' + ynorm(d, protectTotal) + ')'; })
                    .attr('height', function (d) { return ynorm(d, protectTotal); })
                    .attr('width', barWidth);
            }

            if (keepTotal > 0) {
                var offset = d3.transform(bar.select('.protect').attr('transform')).translate[1];
                bar.append('rect')
                    .classed('searched barComponent', true)
                    .attr('transform', function (d) { return 'translate(0,' + (offset - ynorm(d, keepTotal)) + ')'; })
                    .attr('height', function (d) { return ynorm(d, keepTotal); })
                    .attr('width', barWidth);
            }

            if (deleteTotal > 0) {
                var offset = d3.transform(bar.select('.keep').attr('transform')).translate[1];
                bar.append('rect')
                    .classed('searched barComponent', true)
                    .attr('transform', function (d) { return 'translate(0,' + (offset - ynorm(d, deleteTotal)) + ')'; })
                    .attr('height', function (d) { return ynorm(d, deleteTotal); })
                    .attr('width', barWidth);
            }

        });

    // Hide sites that do not have the datasets and print tables containing only the searched datasets
    siteDetails.each(function (d) {
            var x = 0;
            while (x != data.length) {
                if (d.name == data[x].name)
                    break;
                x += 1;
            }

            if (x == data.length) {
                this.style.display = 'none';
                return;
            }

            var siteDatasets = data[x].datasets;

            var tableBox = d3.select(this).select('.siteTableBox');
            tableBox.select('div.loadSiteData').style('display', 'none');
            tableBox.select('tbody.full').style('display', 'none');

            var tbody = addTableRows(tableBox.select('table'), 'searched', siteDatasets);
            var tbodyHeight = tbody.node().clientHeight;
            if (tbodyHeight > 656) {
                tbody.style('height', '656px');
                tableBox.style('height', '700px');
            }
            else
                tableBox.style('height', (tbodyHeight + 44) + 'px');
        });

    if (data.length == 0) {
        d3.select('#details').append('div').classed('searchResult', true)
            .style({'text-align': 'center', 'font-size': '18px', 'margin-bottom': '10px'})
            .text('No replica was found.');
    }
}

function resetDatasetSearch()
{
    d3.selectAll('#summaryGraph rec.searched').remove();
    d3.selectAll('#details div.searchResult').remove();

    siteDetails.style('display', 'block');
    var tableBox = siteDetails.select('.siteTableBox');
    if (tableBox.select('div.loadSiteData').style('display', 'block').size() != 0)
        tableBox.style('height', '82px');
    else if (tableBox.select('tbody.full').style('display', 'block').size() != 0)
        tableBox.style('height', '700px');

    tableBox.select('tbody.searched').remove();
}

function loadSummary(cycleNumber, partitionId, summaryNorm)
{
    currentCycle = cycleNumber;
    currentPartition = partitionId;
    currentNorm = summaryNorm;

    d3.selectAll('.partitionTab')
        .classed('selected', false);
    
    d3.select('#partition' + partitionId)
        .classed('selected', true);

    var box = d3.select('#summaryGraphBox');
    box.selectAll('*').remove();

    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $(box.node()).append($(spinner.el));

    var inputData = {
        'getData': 1,
        'dataType': 'summary',
        'cycleNumber': cycleNumber,
        'partitionId': partitionId
    };

    $.ajax({'url': window.location.href, 'data': inputData, 'success': function (data, textStatus, jqXHR) {
                nextCycle = data.nextCycle;
                previousCycle = data.previousCycle;
                displaySummary(data);
                spinner.stop();
                setupSiteDetails(data.siteData);
            }, 'dataType': 'json', 'async': false});
}

function loadSiteTable(name)
{
    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $('#' + name + ' .siteTableBox').append($(spinner.el));

    var inputData = {
        'getData': 1,
        'dataType': 'siteDetail',
        'cycleNumber': currentCycle,
        'partitionId': currentPartition,
        'siteName': name
    };

    $.get(window.location.href, inputData, function (data, textStatus, jqXHR) {
            displayDetails(data);
            spinner.stop();
    }, 'json');
}

function findDataset()
{
    resetDatasetSearch();

    var datasetName = $('#datasetSearch').val();

    if ($.trim(datasetName) == '')
        return;

    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $('#summaryGraphBox').append($(spinner.el));

    var inputData = {
        'searchDataset': 1,
        'cycleNumber': currentCycle,
        'partitionId': currentPartition,
        'datasetName': datasetName
    };

    $.get(window.location.href, inputData, function (data, textStatus, jqXHR) {
            displayDatasetSearch(data);
            spinner.stop();
    }, 'json');
}
