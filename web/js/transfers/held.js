function initPage()
{
    var jaxInput = {
        'url': dataPath + '/transfers/held/summary',
        'dataType': 'json',
        'success': function (data, textStatus, jqXHR) { displaySummary(data.data); },
        'error': handleError,
        'async': false
    };

    $.ajax(jaxInput);
}

function showCounts(elem, d, property)
{
    if (d.counts[property] == 0)
        return;

    d3.select(elem)
        .attr('onclick', function (d) { return 'loadDetails("' + d.site + '", "' + property + '");'; })
        .style({'cursor': 'pointer', 'text-decoration': 'underline'});
}

function displaySummary(data)
{
    var tableRow = d3.select('#summaryTableBody').selectAll('.siteSummary')
        .data(data)
        .enter()
        .append('tr').classed('siteSummary', true);

    tableRow.append('td').text(function (d, i) { return d.site; });
    tableRow.append('td')
        .text(function (d, i) { return d.counts.no_source; })
        .each(function (d, i) { showCounts(this, d, "no_source"); });
    tableRow.append('td')
        .text(function (d, i) { return d.counts.all_failed; })
        .each(function (d, i) { showCounts(this, d, "all_failed"); });
    tableRow.append('td')
        .text(function (d, i) { return d.counts.site_unavailable; })
        .each(function (d, i) { showCounts(this, d, "site_unavailable"); });
    tableRow.append('td')
        .text(function (d, i) { return d.counts.unknown; })
        .each(function (d, i) { showCounts(this, d, "unknown"); });
}

function loadDetails(site, reason)
{
    var jaxInput = {
        'url': dataPath + '/transfers/held/detail',
        'data': {'site': site, 'reason': reason},
        'dataType': 'json',
        'success': function (data, textStatus, jqXHR) { displayDetails(data.data); },
        'error': handleError,
        'async': false
    };

    $.ajax(jaxInput);
}

function displayDetails(data)
{
    d3.select('#subscriptionList').style('display', 'table');
    d3.select('#subscriptionListBody').selectAll('.subscription').remove();

    var tableRow = d3.select('#subscriptionListBody').selectAll('.subscription')
        .data(data)
        .enter()
        .append('tr').classed('subscription', true);
    
    tableRow.append('td').text(function (d, i) { return d.id; });
    tableRow.append('td').text(function (d, i) { return d.file; });
    tableRow.append('td').text(function (d, i) { return d.destination; });
    tableRow.append('td').text(function (d, i) { return d.reason; });
    tableRow.append('td').text(function (d, i) { if (d.reason == "all_failed") return d.source; else return ''; });
    tableRow.append('td').text(function (d, i) { if (d.reason == "all_failed") return d.exitcode; else return ''; });
}
