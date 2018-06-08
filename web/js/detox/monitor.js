var currentCycle = 0;
var nextCycle = 0;
var previousCycle = 0;
var latestCycle = 0;
var currentPartition = 0;
var currentPartitionName = '';
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
var conditionTexts = {'0': 'No policy match'};

var datasetSearchColors = [
    '#ff00ff',
    '#ffff66',
    '#ff99ff',
    '#0066ff'
];

var dataPath = window.location.pathname.replace('web', 'data');

function initPage(cycleNumber, partitionId)
{
    // confirm the specified cycle or get the latest
    var cycleInput = {
        'url': dataPath + '/cycles',
        'data': {'partitionId': partitionId, 'cycle': cycleNumber},
        'dataType': 'json',
        'async': true
    };

    var cycleCall = $.ajax(cycleInput);

    var partitionInput = {
        'url': dataPath + '/partitions',
        'data': {},
        'dataType': 'json',
        'async': true
    };

    var partitionCall = $.ajax(partitionInput);

    // initialize the page when both requests return
    $.when(cycleCall, partitionCall).then(function (cycleData, partitionData) {
        currentCycle = cycleData[0].cycle;
        if (cycleNumber == 0) {
            latestCycle = currentCycle;

            // should this partition be monitored?
            for (var x in partitionData) {
                if (partitionData[x]['id'] == partitionId) {
                    if (partitionData[x].monitored)
                        setInterval(checkUpdates, 300000);
                    break;
                }
            }
        }

        setPartitions(partitionData);
        
        loadSummary(cycleNumber, partitionId, currentNorm);
    });
}

function checkUpdates()
{
    if (currentCycle != latestCycle)
        return;

    var jaxData = {
        'url': dataPath + '/cycles',
        'data': {'partitionId': partitionId, 'cycle': 0},
        'success': function (cycleData, textStatus, jqXHR) { processUpdates(cycleData); },
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

function processUpdates(cycleData)
{
    if (cycleData[0]['cycle'] == latestCycle) {
        // is the timestamp old?
        if (cycleData[0].timestamp < (Date.now() / 1000 - 3600 * 18))
            d3.select('#cycleHeader').style('color', 'red');
        return;
    }

    latestCycle = cycleData['cycle'];
    d3.select('#cycleHeader').append('div')
        .text('New cycle ' + latestCycle + ' is available')
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
                    (d.delete + d.keep + d.protect).toFixed(1) + ' TB Total';
                if (d.quota >= 0.)
                    text += ' / Quota ' + d.quota + ' TB)';
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
            .on('click', function () { window.open('https://github.com/SmartDataProjects/dynamo-policies/blob/' + this.textContent + '/detox/' + currentPartitionName + '.txt'); });
    }
    else {
        cycleHeader.append('span').text('unknown');
    }
    var local = new Date(); // add the timezone offset to the unix timestamp to get at UTC
    var timestamp = new Date((data.cycleTimestamp + local.getTimezoneOffset() * 60) * 1000);
    cycleHeader.append('span').text(', ' + d3.time.format('%Y-%m-%d %H:%M:%S UTC')(timestamp) + ')');

    if (data.cycleTimestamp < (local.getTime() / 1000 - 3600 * 18))
        cycleHeader.style('color', 'red');

    d3.select('#cycleComments')
        .append('span').text(data.comment);

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

    var onlyAbsolute = false;
    for (var s in data.siteData) {
        if (data.siteData[s].quota <= 0) {
            currentNorm = "absolute";
            titleRelative.style('fill', '#808080');
            selectRelative.attr('stroke', '#808080');
            onlyAbsolute = true;
            break;
        }
    }

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

        if (!onlyAbsolute) {
            selectRelative.attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');

            titleRelative.style('cursor', 'pointer')
                .attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');
        }

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
            .attr('transform', function (d) { return 'translate(' + xmapping(d.name) + ',0)'; })
            .each(function (d) {
                    if (d.quota <= 0)
                        return;

                    d3.select(this).append('line').classed('refMarker quota', true)
                    .attr({'y1': -ynorm(d.quota), 'y2': -ynorm(d.quota)});

                    d3.select(this).append('line').classed('refMarker trigger', true)
                    .attr({'y1': -ynorm(d.quota * 0.9), 'y2': -ynorm(d.quota * 0.9)});

                    d3.select(this).append('line').classed('refMarker target', true)
                    .attr({'y1': -ynorm(d.quota * 0.85), 'y2': -ynorm(d.quota * 0.85)});
                });

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
                if (d.quota <= 0)
                    return;

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
                if (d.quota <= 0)
                    return;

                if (d.protect > d.quota)
                    this.style.fill = '#ff0000';
                else if (d.protect > d.quota * 0.9)
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

    // total separator
    content.append('line').classed('totalSeparator', true)
        .attr('x1', xmapping('Total') + xspace * 0.5)
        .attr('x2', xmapping('Total') + xspace * 0.5)
        .attr('y1', 0.)
        .attr('y2', -summary.ymax)
        .attr({'stroke-dasharray': '0.2,0.2', 'stroke-width': 0.2, 'stroke': 'black'});

    var lineLegend = summaryGraph.append('g').classed('lineLegend', true)
        .attr('transform', 'translate(' + gxnorm(50) + ', 0)');

    var lineLegendContents = 
        [{'cls': 'quota', 'title': 'Quota', 'position': '(0,3.5)'},
         {'cls': 'trigger', 'title': 'Deletion trigger', 'position': '(0,7.5)'},
         {'cls': 'target', 'title': 'Target occupancy', 'position': '(0,11.5)'}];

    var lineLegendEntries = lineLegend.selectAll('g')
        .data(lineLegendContents)
        .enter()
        .append('g')
        .attr('transform', function (d) { return 'translate' + d.position });

    lineLegendEntries.append('line')
        .attr('class', function (d) { return d.cls; })
        .classed('refMarker', true)
        .attr({'x1': 0, 'x2': gxnorm(2), 'y1': 1, 'y2': 1});

    lineLegendEntries.append('text')
        .attr({'font-size': 2, 'dx': gxnorm(3), 'dy': 2})
        .text(function (d) { return d.title; });

    var legend = summaryGraph.append('g').classed('legend', true)
        .attr('transform', 'translate(' + gxnorm(64) + ', 0)');

    var total_deleted = 0;
    var total_kept = 0;
    var total_protected = 0;
    for (var x in data.siteData) {
        if (data.siteData[x].name == 'Total')
            continue;
        total_deleted += data.siteData[x].delete;
        total_kept += data.siteData[x].keep;
        total_protected += data.siteData[x].protect;
    }
    var title_deleted = 'Deleted (';
    var title_kept = 'Kept (';
    var title_protected = 'Protected (';
    if (total_deleted < 100)
        title_deleted += total_deleted.toFixed(1) + ' TB)';
    else
        title_deleted += (total_deleted * 1.e-3).toFixed(1) + ' PB)';
    if (total_kept < 100)
        title_kept += total_kept.toFixed(1) + ' TB)';
    else
        title_kept += (total_kept * 1.e-3).toFixed(1) + ' PB)';
    if (total_protected < 100)
        title_protected += total_protected.toFixed(1) + ' TB)';
    else
        title_protected += (total_protected * 1.e-3).toFixed(1) + ' PB)';

    var legendContents =
        [{'cls': 'delete', 'title': title_deleted, 'position': '(0,3.5)'},
         {'cls': 'keep', 'title': title_kept, 'position': '(0,7.5)'},
         {'cls': 'protect', 'title': title_protected, 'position': '(0,11.5)'},
         {'cls': 'keepPrev', 'title': 'Kept in previous cycle', 'position': '(' + gxnorm(16) + ',3.5)'},
         {'cls': 'protectPrev', 'title': 'Protected in previous cycle', 'position': '(' + gxnorm(16) + ',7.5)'}];

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
        .text(function (d) { if ('reason' in d) return d.reason; else return conditionTexts[d.conditionId]; });

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
    // data: [{siteData: [name: site_name, datasets: [{name: dataset_name, size: size, decision: dec, reason: reason}]]}]
    // one element per search pattern

    // Add bars corresponding to searched datasets to the summary graph
    var ynorm;

    if (currentNorm == 'relative')
        ynorm = function (site, val) { if (site.quota <= 0.) return 0.; else return summary.ymax - summary.yscale(val / site.quota); }
    else
        ynorm = function (site, val) { return (summary.ymax - summary.yscale(val)) / 1000.; }

    var siteAllDatasets = {};

    // list of lists; outer: search pattern, inner: site
    var protectOffsets = [];
    var keepOffsets = [];
    var deleteOffsets = [];

    for (var ipat in data) {
        var siteData = data[ipat].siteData;

        var protectOffsetsNext = [];
        var keepOffsetsNext = [];
        var deleteOffsetsNext = [];

        var color = datasetSearchColors[ipat % datasetSearchColors.length];

        summary.bars.each(function (site, isite) {
                protectOffsetsNext.push(0);
                keepOffsetsNext.push(0);
                deleteOffsetsNext.push(0);

                var x = 0;
                while (x != siteData.length) {
                    if (site.name == siteData[x].name)
                        break;
                    x += 1;
                }
    
                if (x == siteData.length)
                    return;

                var protectTotal = 0;
                var keepTotal = 0;
                var deleteTotal = 0;

                if (site.name == 'Total') {
                    protectTotal = siteData[x].protect / 1000.;
                    keepTotal = siteData[x].keep / 1000.;
                    deleteTotal = siteData[x].delete / 1000.;
                }
                else {
                    var siteDatasets = siteData[x].datasets;
                    if (siteAllDatasets[site.name] === undefined)
                        siteAllDatasets[site.name] = siteDatasets;
                    else
                        siteAllDatasets[site.name] = siteAllDatasets[site.name].concat(siteDatasets);
    
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
                }
    
                var bar = d3.select(this);
                var barWidth = bar.select('.barComponent').attr('width');
    
                if (protectTotal > 0) {
                    var offset = 0;
                    if (ipat != 0)
                        offset = protectOffsets[ipat - 1][isite];

                    var height = ynorm(site, protectTotal);
                    var zero = offset - height;
                    // this will become the offset for the next dataset
                    protectOffsetsNext[isite] = zero;

                    bar.append('rect')
                        .classed('barComponent searched', true)
                        .attr('fill', color)
                        .attr('transform', 'translate(0,' + zero + ')')
                        .attr('height', height)
                        .attr('width', barWidth);
                }
    
                if (keepTotal > 0) {
                    var offset;
                    if (ipat == 0)
                        offset = d3.transform(bar.select('.protect').attr('transform')).translate[1];
                    else
                        offset = keepOffsets[ipat - 1][isite];

                    var height = ynorm(site, keepTotal);
                    var zero = offset - height;
                    // this will become the offset for the next dataset
                    keepOffsetsNext[isite] = zero;

                    bar.append('rect')
                        .classed('barComponent searched', true)
                        .attr('fill', color)
                        .attr('transform', 'translate(0,' + zero + ')')
                        .attr('height', height)
                        .attr('width', barWidth);
                }
    
                if (deleteTotal > 0) {
                    var offset;
                    if (ipat == 0)
                        offset = d3.transform(bar.select('.keep').attr('transform')).translate[1];
                    else
                        offset = deleteOffsets[ipat - 1][isite];

                    var height = ynorm(site, deleteTotal);
                    var zero = offset - height;
                    // this will become the offset for the next dataset
                    deleteOffsetsNext[isite] = zero;

                    bar.append('rect')
                        .classed('barComponent searched', true)
                        .attr('fill', color)
                        .attr('transform', 'translate(0,' + zero + ')')
                        .attr('height', height)
                        .attr('width', barWidth);
                }
    
            });

        var displayBox = d3.select('#datasetsOnDisplay').append('div')
            .classed('displayed', true)
            .style({'width': '100%', 'height': '25px'});

        displayBox.append('div')
            .classed('datasetSearchIndex', true)
            .append('div')
            .style({'width': '20px', 'height': '20px', 'background-color': color, 'margin-right': '5px', 'float': 'right'});

        var inputs = displayBox.append('div')
            .classed('datasetSearchInputs', true);

        inputs.append('div')
            .classed('datasetSearch', true)
            .style({'margin-left': '2%', 'float': 'left'})
            .text(data[ipat].pattern);

        inputs.append('input')
            .classed('datasetSearchButton', true)
            .property('type', 'button')
            .property('value', 'Remove')
            .on('click', function () { removeDataset(this.parentNode.parentNode) });

        protectOffsets.push(protectOffsetsNext);
        keepOffsets.push(keepOffsetsNext);
        deleteOffsets.push(deleteOffsetsNext);
    }

    d3.select('#datasetsSearchNav').style('height', (25 * (data.length + 1)) + 10 + 'px');
    d3.select('#datasetsOnDisplay').style('height', (25 * data.length) + 'px');

    // Hide sites that do not have the datasets and print tables containing only the searched datasets
    if (siteAllDatasets.length == 0) {
        d3.select('#details').append('div').classed('searchResult', true)
            .style({'text-align': 'center', 'font-size': '18px', 'margin-bottom': '10px'})
            .text('No replica was found.');
    }
    else {
        siteDetails.each(function (site, isite) {
                if (siteAllDatasets[site.name] === undefined) {
                    this.style.display = 'none';
                    return;
                }

                var tableBox = d3.select(this).select('.siteTableBox');
                tableBox.select('div.loadSiteData').style('display', 'none');
                tableBox.select('tbody.full').style('display', 'none');

                var tbody = addTableRows(tableBox.select('table'), 'searched', siteAllDatasets[site.name]);
                var tbodyHeight = tbody.node().clientHeight;
                if (tbodyHeight > 656) {
                    tbody.style('height', '656px');
                    tableBox.style('height', '700px');
                }
                else
                    tableBox.style('height', (tbodyHeight + 44) + 'px');
            });
    }
}

function resetDatasetSearch()
{
    d3.selectAll('#summaryGraph rect.searched').remove();
    d3.selectAll('#details div.searchResult').remove();

    siteDetails.style('display', 'block');
    var tableBox = siteDetails.select('.siteTableBox');
    if (tableBox.select('div.loadSiteData').style('display', 'block').size() != 0)
        tableBox.style('height', '82px');
    else if (tableBox.select('tbody.full').style('display', 'block').size() != 0)
        tableBox.style('height', '700px');

    tableBox.select('tbody.searched').remove();

    d3.selectAll('#datasetsOnDisplay div.displayed').remove();

    d3.select('#datasetsSearchNav').style('height', '35px');
    d3.select('#datasetsOnDisplay').style('height', 0);
}

function loadSummary(cycleNumber, partitionId, summaryNorm)
{
    currentCycle = cycleNumber;
    currentPartition = partitionId;
    currentNorm = summaryNorm;

    d3.selectAll('.partitionTab')
        .classed('selected', false);
    
    var currentTab = d3.select('#partition' + partitionId);
    currentTab.classed('selected', true);
    currentPartitionName = currentTab.text();

    d3.select('#cycleHeader').selectAll('span').remove();
    d3.select('#cycleComments').selectAll('span').remove();

    var box = d3.select('#summaryGraphBox');
    box.selectAll('*').remove();

    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $(box.node()).append($(spinner.el));

    var jaxData = {
        'url': dataPath + '/summary',
        'data': {'cycle': cycleNumber},
        'success': function (data, textStatus, jqXHR) {
            nextCycle = data.nextCycle;
            previousCycle = data.previousCycle;
            displaySummary(data);
            setDetailsLink(data);
            spinner.stop();
            setupSiteDetails(data.siteData.slice(1)); // 0th element is Total
        },
        'dataType': 'json',
        'async': false
    }

    $.ajax(jaxData);

    if (previousCycle == 0)
        d3.select('#previous').classed('clickable', false).on('click', null);
    else
        d3.select('#previous').classed('clickable', true).on('click', function () { loadSummary(previousCycle, currentPartition, currentNorm); });

    if (nextCycle == 0)
        d3.select('#next').classed('clickable', false).on('click', null);
    else
        d3.select('#next').classed('clickable', true).on('click', function () { loadSummary(nextCycle, currentPartition, currentNorm); });
}

function setDetailsLink(data)
{
    var hasDelete = false;

    for (var x in data.siteData) {
        if (data.siteData[x].delete != 0.) {
            hasDelete = true;
            break;
        }
    }

    if (hasDelete)
        d3.select('#download').classed('clickable', true)
            .on('click', function () { downloadList(); });
    else
        d3.select('#download').classed('clickable', false)
            .on('click', null);
}

function loadSiteTable(name)
{
    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $('#' + name + ' .siteTableBox').append($(spinner.el));

    var jaxData = {
        'url': dataPath + '/sitedetail',
        'data': {'cycle': cycleNumber, 'site': name},
        'success': function (data, textStatus, jqXHR) {
            for (var cid in data.conditions) {
                if (!(cid in conditionTexts))
                    conditionTexts[cid] = data.conditions[cid];
            }

            displayDetails(data.content);
            spinner.stop();
        },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
}

function findDataset()
{
    var inputBox = $('#newDatasetSearch');

    var input = $.trim(inputBox.val());
    if (input == '')
        return;

    var datasetNames = [];

    d3.select('#datasetsOnDisplay').selectAll('div.displayed div.datasetSearch')
        .each(function() { datasetNames.push(this.innerHTML); });

    datasetNames.push(input);

    resetDatasetSearch();

    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $('#summaryGraphBox').append($(spinner.el));

    var jaxData = {
        'url': dataPath + '/datasets',
        'data': {'cycle': cycleNumber, 'datasets': datasetNames},
        'success': function (data, textStatus, jqXHR) {
            for (var cid in data.conditions) {
                if (!(cid in conditionTexts))
                    conditionTexts[cid] = data.conditions[cid];
            }

            inputBox.val('');
            displayDatasetSearch(data.results);
            spinner.stop();
        },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
}

function removeDataset(displayBox)
{
    var datasetNames = [];

    d3.select('#datasetsOnDisplay').selectAll('div.displayed')
        .each(function() {
                if (this != displayBox)
                    datasetNames.push(d3.select(this).select('div.datasetSearch').html());
            });

    resetDatasetSearch();

    if (datasetNames.length == 0)
        return;

    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'absolute'});
    spinner.spin();
    $('#summaryGraphBox').append($(spinner.el));

    var jaxData = {
        'url': dataPath + '/datasets',
        'data': {'cycle': cycleNumber, 'datasets': datasetNames},
        'success': function (data, textStatus, jqXHR) {
            for (var cid in data.conditions) {
                if (!(cid in conditionTexts))
                    conditionTexts[cid] = data.conditions[cid];
            }

            inputBox.val('');
            displayDatasetSearch(data.results);
            spinner.stop();
        },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
}

function downloadList()
{
    var url = window.location.href.split('?')[0];
    url += '?command=dumpDeletions';
    url += '&cycleNumber=' + currentCycle;
    
    window.location = url;
}
