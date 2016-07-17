var currentCycle = 0;
var nextCycle = 0;
var previousCycle = 0;
var currentPartition = 0;
var currentNorm = 'relative';

function initPage(cycleNumber, partitionId)
{
    var jaxData = {
        'url': 'detox.php',
        'data': {'getPartitions': 1},
        'success': function (data, textStatus, jqXHR) { setPartitions(data); },
        'dataType': 'json',
        'async': false};

    $.ajax(jaxData);
    
    loadSummary(cycleNumber, partitionId, currentNorm, true);
}

function setPartitions(data)
{
    var partitionsNav = d3.select('#partitions');
    partitionsNav.selectAll('.partitionTab')
        .data(data)
        .enter().append('div').classed('partitionTab', true)
        .text(function (d) { return d.name; })
        .attr('id', function (d) { return 'partition' + d.id; })
        .on('click', function (d) { loadSummary(currentCycle, d.id, currentNorm); });

    partitionsNav.select(':last-child').classed('last', true);
}

function setupSiteDetails(siteData)
{
    // set up tables for individual sites

    d3.select('#details').selectAll('.siteDetails').remove();
    
    var siteDetails = d3.select('#details').selectAll('.siteDetails')
        .data(siteData)
        .enter()
        .append('article').classed('siteDetails', true)
        .attr('id', function (d) { return d.name; });

    // everything that is selected from siteDetails will have data = site data

    siteDetails.append('h3').classed('siteName', true)
        .text(function (d) {
                var text = d.name + ' (';
                text += d.delete.toFixed(1) + ' TB Delete, ';
                text += d.keep.toFixed(1) + ' TB Keep, ';
                text += d.protect.toFixed(1) + ' TB Protect, ';
                text += (d.delete + d.keep + d.protect).toFixed(1) + ' TB Total)';
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

    tableBox.append('div')
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
    d3.select('#cycleNumber').text(data.cycleNumber);
    d3.select('#cycleTimestamp').text(data.cycleTimestamp);

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

    var xorigin = 6;
    var yorigin = 74;
    var xmax = 100 - xorigin;
    var ymarginTop = 12;
    var ymarginBottom = 100 - yorigin;
    var ymax = yorigin - ymarginTop;

    var xmapping = d3.scale.ordinal()
        .domain(data.siteData.map(function (v) { return v.name; }))
        .rangePoints([0, gxnorm(xmax)], 1);

    var xspace = gxnorm(xmax / data.siteData.length);

    var yscale = d3.scale.linear();

    var ynorm;

    var titleRelative = summaryGraph.append('text')
        .attr({'font-size': 3, 'x': gxnorm(xorigin + 3), 'y': 4})
        .text('Normalized site usage');

    var selectRelative = summaryGraph.append('circle')
        .attr({'cx': gxnorm(xorigin + 2), 'cy': 3, 'r': 1, 'stroke': 'black', 'stroke-width': 0.3, 'fill': 'white'});

    var titleAbsolute = summaryGraph.append('text')
        .attr({'font-size': 3, 'x': gxnorm(xorigin + 3), 'y': 8})
        .text('Absolute data volume');

    var selectAbsolute = summaryGraph.append('circle')
        .attr({'cx': gxnorm(xorigin + 2), 'cy': 7, 'r': 1, 'stroke': 'black', 'stroke-width': 0.3, 'fill': 'white'});

    var eye = summaryGraph.append('circle')
        .attr({'fill': 'black', 'cx': gxnorm(xorigin + 2), 'r': 0.6});

    if (currentNorm == 'relative') {
        eye.attr('cy', 3);

        selectAbsolute.attr('onclick', 'loadSummary(currentCycle, currentPartition, \'absolute\');');

        titleAbsolute.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentCycle, currentPartition, \'absolute\');');

        yscale.domain([0, 1.25])
            .range([ymax, 0]);

        ynorm = function (d, key) { if (d.quota == 0.) return 0.; else return ymax - yscale(d[key] / d.quota); };
    }
    else {
        eye.attr('cy', 7);

        selectRelative.attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');

        titleRelative.style('cursor', 'pointer')
            .attr('onclick', 'loadSummary(currentCycle, currentPartition, \'relative\');');

        yscale.domain([0, d3.max(data.siteData, function (d) { return Math.max(d.protect + d.keep + d.delete, d.protectPrev + d.keepPrev) / 1000.; }) * 1.25])
            .range([ymax, 0]);

        ynorm = function (d, key) { return (ymax - yscale(d[key])) / 1000.; };
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
        .attr('transform', 'translate(' + gxnorm(xorigin) + ',' + yorigin + ')')
        .call(xaxis);

    gxaxis.selectAll('.tick text')
        .attr({'font-size': 2, 'dx': gxnorm(-0.2), 'dy': -1.4, 'transform': 'rotate(300 0,0)'})
        .attr('onclick', function (siteName) { return 'd3.select(\'#' + siteName + '\').node().scrollIntoView();'; })
        .style({'text-anchor': 'end', 'cursor': 'pointer'});

    gxaxis.select('path.domain')
        .attr({'fill': 'none', 'stroke': 'black', 'stroke-width': 0.2});

    var gyaxis = summaryGraph.append('g').classed('axis', true)
        .attr('transform', 'translate(' + gxnorm(xorigin) + ',' + ymarginTop + ')')
        .call(yaxis);

    gyaxis.selectAll('.tick text')
        .attr('font-size', 3);

    gyaxis.selectAll('.tick line')
        .attr({'stroke': 'black', 'stroke-width': 0.2});

    gyaxis.select('path.domain')
        .attr({'fill': 'none', 'stroke': 'black', 'stroke-width': 0.2});

    var content = summaryGraph.append('g').classed('content', true)
        .attr('transform', 'translate(' + gxnorm(xorigin) + ',' + yorigin + ')');

    if (currentNorm == 'relative') {
        content.append('line').classed('refMarker', true)
            .attr({'x1': 0, 'x2': gxnorm(xmax), 'y1': -ymax / 1.25, 'y2': -ymax / 1.25});

        content.append('line').classed('refMarker', true)
            .attr({'x1': 0, 'x2': gxnorm(xmax), 'y1': -ymax * 0.5 / 1.25, 'y2': -ymax * 0.5 / 1.25, 'stroke-dasharray': '3,3'});

        content.append('line').classed('refMarker', true)
            .attr({'x1': 0, 'x2': gxnorm(xmax), 'y1': -ymax * 0.9 / 1.25, 'y2': -ymax * 0.9 / 1.25, 'stroke-dasharray': '1,1'});

        content.selectAll('.refMarker')
            .attr({'stroke': 'black', 'stroke-width': 0.2});
    }
    else {
        gyaxis.append('text')
            .attr({'transform': 'translate(' + gxnorm(-2) + ',-2)', 'font-size': 3})
            .text('PB');
    }

    var barPrev = content.selectAll('.barPrev')
        .data(data.siteData)
        .enter()
        .append('g').classed('barPrev', true)
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) - xspace * 0.325) + ',0)'; })
        .attr('onclick', function (d) { return 'd3.select(\'#' + d.name + '\').node().scrollIntoView();'; })
        .style('cursor', 'pointer');

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
        .attr('transform', function (d) { return 'translate(' + (xmapping(d.name) + xspace * 0.025) + ',0)'; })
        .attr('onclick', function (d) { return 'd3.select(\'#' + d.name + '\').node().scrollIntoView();'; })
        .style('cursor', 'pointer');

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
        .attr('transform', 'translate(' + gxnorm(68) + ', 0)');

    var legendContents =
        [{'cls': 'delete', 'title': 'Deleted', 'position': '(0,4)'},
         {'cls': 'keep', 'title': 'Kept', 'position': '(0,8)'},
         {'cls': 'protect', 'title': 'Protected', 'position': '(0,12)'},
         {'cls': 'keepPrev', 'title': 'Kept in previous cycle', 'position': '(' + gxnorm(10) + ',4)'},
         {'cls': 'protectPrev', 'title': 'Protected in previous cycle', 'position': '(' + gxnorm(10) + ',8)'}];

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
    
    var tbody = table.append('tbody')
        .style({'height': '656px', 'overflow-y': 'auto', 'overflow-x': 'hidden'});

    var row = tbody.selectAll('tr')
        .data(siteData.datasets)
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
}

function loadSummary(cycleNumber, partitionId, summaryNorm, initial)
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

    $.ajax({'url': 'detox.php', 'data': inputData, 'success': function (data, textStatus, jqXHR) {
                nextCycle = data.nextCycle;
                previousCycle = data.previousCycle;
                displaySummary(data);
                spinner.stop();
                if (initial)
                    setupSiteDetails(data.siteData);
            }, 'dataType': 'json', 'async': false});
}

function loadSiteTable(name)
{
    var spinner = new Spinner({'scale': 5, 'corners': 0, 'width': 2, 'position': 'relative'});
    spinner.spin();
    $('#' + name + ' .siteTableBox').append($(spinner.el));

    var inputData = {
        'getData': 1,
        'dataType': 'siteDetail',
        'cycleNumber': currentCycle,
        'partitionId': currentPartition,
        'siteName': name
    };

    $.get('detox.php', inputData, function (data, textStatus, jqXHR) {
            displayDetails(data);
            spinner.stop();
    }, 'json');
}
