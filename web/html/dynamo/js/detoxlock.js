function initPage()
{
    var jaxData = {
        'url': window.location.href,
        'data': {'data': 1},
        'success': function (data, textStatus, jqXHR) { showLockData(data); },
        'dataType': 'json',
        'async': false
    };

    $.ajax(jaxData);
}

function showLockData(data)
{
    var enabledRows = d3.select('#enabled table tbody').selectAll('tr')
        .data(data.enabled)
        .enter().append('tr');

    enabledRows.append('td')
        .text(function (d) { return d.user; });
    enabledRows.append('td')
        .text(function (d) { return d.item; });
    enabledRows.append('td')
        .text(function (d) { return d.sites; });
    enabledRows.append('td')
        .text(function (d) { return d.groups; });
    enabledRows.append('td')
        .text(function (d) { return d.entryDate; });
    enabledRows.append('td')
        .text(function (d) { return d.expirationDate; });
    enabledRows.append('td')
        .text(function (d) { return d.comment; });

    var disabledRows = d3.select('#disabled table tbody').selectAll('tr')
        .data(data.disabled)
        .enter().append('tr');

    disabledRows.append('td')
        .text(function (d) { return d.user; });
    disabledRows.append('td')
        .text(function (d) { return d.item; });
    disabledRows.append('td')
        .text(function (d) { return d.sites; });
    disabledRows.append('td')
        .text(function (d) { return d.groups; });
    disabledRows.append('td')
        .text(function (d) { return d.entryDate; });
    disabledRows.append('td')
        .text(function (d) { return d.expirationDate; });
    disabledRows.append('td')
        .text(function (d) { return d.comment; });
}
