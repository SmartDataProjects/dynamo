function selectPage(run, partition)
{
    $('#run_number').val(run);
    $('#partition_id').val(partition);
    $('#selectPage').get(0).submit();
}

function initPage(data)
{
    var partitions = $('#partitions');
    var partitionName = '';
    for (var iP in data.partitions) {
        var partition = data.partitions[iP];
        var tab = $(document.createElement('div'));
        tab.addClass('partitionTab');
        tab.addClass('clickable');
        if (partition.id == data.partition) {
            tab.addClass('selectedPartition');
            partitionName = partition.name;
        }
        tab.click(function() { selectPage(data.run, partition.id); });
        tab.html(partition.name);
        partitions.append(tab);
    }

    $('#partitionName').html(partitionName);

    var graphArea = $('#graphs');
    var graphAreaHeight = $('#summaryPanel').height;
    var protFraction = [];
    var keepFraction = [];
    var deltFraction = [];

    for (var iS in data.sites) {
        var site = data.sites[iS];

        var sitePanel = $(document.createElement('article'));
        sitePanel.addClass('sitePanel');
        if (iS % 2 == 0)
            sitePanel.addClass('even');
        else
            sitePanel.addClass('odd');

        graphArea.append(sitePanel);

        if (iS % (graphArea.width() / sitePanel.outerWidth()) == 0)
            graphAreaHeight += (sitePanel.outerHeight());

        protDatasets[site.id] = [];
        keepDatasets[site.id] = [];
        deltDatasets[site.id] = [];
        protFraction[site.id] = 0.;
        keepFraction[site.id] = 0.;
        deltFraction[site.id] = 0.;

        for (var iR in data.replicas[site.id]) {
            var replica = data.replicas[site.id][iR];

            if (replica.dec == 'protect') {
                protDatasets[site.id].push(replica.ds);
                protFraction[site.id] += replica.size;
            }
            else if (replica.dec == 'keep') {
                keepDatasets[site.id].push(replica.ds);
                keepFraction[site.id] += replica.size;
            }
            else {
                deltDatasets[site.id].push(replica.ds);
                deltFraction[site.id] += replica.size;
            }
        }

        protFraction[site.id] /= data.quotas[site.id];
        keepFraction[site.id] /= data.quotas[site.id];
        deltFraction[site.id] /= data.quotas[site.id];
    }

    graphArea.height(graphAreaHeight);

    var summaryGraph = d3.select('#summaryGraph');

    var x0 = 8.; // origin
    var y0 = 70.; // origin
    var y1 = 10.; // line 1

    var summaryNorm = y0 - y1;
    var summaryColumn = summaryGraph.selectAll('.summaryColumn')
        .data(data.sites)
        .enter().append('g');

    summaryColumn.append('rect').classed('outline')
        .attr('x', function(s, i) { return x0 + i * 4. + 0.25; })
        .attr('y', function(s) { return y0 - (protFraction[s.id] + keepFraction[s.id] + deltFraction[s.id]) * summaryNorm; })
        .attr('width', 3.5)
        .attr('height', function(s) { return (protFraction[s.id] + keepFraction[s.id] + deltFraction[s.id]) * summaryNorm; });

    summaryColumn.append('rect').classed('protect', true)
        .attr('x', function(s, i) { return x0 + i * 4. + 0.26; })
        .attr('y', function(s) { return y0 - protFraction[s.id] * summaryNorm; })
        .attr('width', 3.48)
        .attr('height', function(s) { return protFraction[s.id] * summaryNorm; });

    summaryColumn.append('rect').classed('keep', true)
        .attr('x', function(s, i) { return x0 + i * 4. + 0.26; })
        .attr('y', function(s) { return y0 - (protFraction[s.id] + keepFraction[s.id]) * summaryNorm; })
        .attr('width', 3.48)
        .attr('height', function(s) { return keepFraction[s.id] * summaryNorm; });

    summaryColumn.append('rect').classed('delete', true)
        .attr('x', function(s, i) { return x0 + i * 4. + 0.26; })
        .attr('y', function(s) { return y0 - (protFraction[s.id] + keepFraction[s.id] + deltFraction[s.id]) * summaryNorm; })
        .attr('width', 3.48)
        .attr('height', function(s) { return deltFraction[s.id] * summaryNorm; });

    summaryColumn.append('text').classed('siteName', true)
        .attr('x', function(s, i) { return x0 + i * 4.; })
        .attr('y', y0 - 0.5)
        .attr('text-anchor', 'end')
        .text(function(s) { return s.name; });

    // x axis
    summaryGraph.append('line').classed('graphAxis', true)
        .attr('x1', 3)
        .attr('x2', 197)
        .attr('y1', y0)
        .attr('y2', y0);

    // y axis
    summaryGraph.append('line').classed('graphAxis', true)
        .attr('x1', x0)
        .attr('x2', x0)
        .attr('y1', 3)
        .attr('y2', 97);

    // unity
    summaryGraph.append('line')
        .attr('x1', 8)
        .attr('x2', 197)
        .attr('y1', y1)
        .attr('y2', y1)
        .style({stroke: 'dashed', stroke-width: 0.1});

    // 0.5
    summaryGraph.append('line')
        .attr('x1', 8)
        .attr('x2', 197)
        .attr('y1', y0 - (y0 - y1) * 0.5)
        .attr('y2', y0 - (y0 - y1) * 0.5)
        .style({stroke: 'dashed', stroke-width: 0.1});

    var graphArea = d3.select('#graphs');
    var sitePanel = graphArea.selectAll('.sitePanel')
        .data(data.sites)
        .attr('id', function(s) { return 'panel_' + s.id; });

    sitePanel.append('h3').classed('panelTitle', true)
        .text(function(s) { return s.name; });

    var graph = sitePanel.append('svg').classed('graph', true);

    var gHeight = 350;

    graph.append('rect')
        .classed('protected', true)
        .attr('x', 0)
        .attr('y', gHeight)
        .attr('height', function(s) { return gHeight * protFraction[s.id]; });
    
    graph.append('rect')
        .classed('keep', true)
        .attr('x', 0)
        .attr('y', function(s) { return gHeight * (1. - protFraction[s.id]); })
        .attr('height', function(s) { return gHeight * keepFraction[s.id]; });

    graph.append('rect')
        .classed('delete', true)
        .attr('x', 0)
        .attr('y', function(s) { return gHeight * (1. - protFraction[s.id] - keepFraction[s.id]); })
        .attr('height', function(s) { return gHeight * deltFraction[s.id]; });
    
}
