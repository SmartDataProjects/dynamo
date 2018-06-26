function initPage()
{
  var jaxInput = {
    'url': dataPath + '/transfers/current',
    'dataType': 'json',
    'success': function (data, textStatus, jqXHR) { displayTable(data.data); },
    'error': handleError,
    'async': false
  };

  $.ajax(jaxInput);
}

function displayTable(data)
{
  var tableRow = d3.select('#transferListBody').selectAll('.transfer')
    .data(data)
    .enter()
    .append('tr').classed('transfer', true);

  tableRow.append('td').each(function (d, i) { this.innerHTML = d.id; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = d.from; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = d.to; });
  tableRow.append('td').classed('lfn', true).each(function (d, i) { this.innerHTML = d.lfn; });
  tableRow.append('td').each(function (d, i) { var size = d.size * 1.e-9; this.innerHTML = size.toFixed(2); });
  tableRow.append('td').each(function (d, i) { this.innerHTML = d.status; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = d.start; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = d.finish; });
}
