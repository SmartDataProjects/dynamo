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
  var tableRow = d3.select('#contents').selectAll('.transfer')
    .data(data)
    .enter()
    .append('tr').classed('transfer', true);

  tableRow.append('td').each(function (d, i) { this.innerHTML = data.id; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.from; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.to; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.lfn; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.status; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.start; });
  tableRow.append('td').each(function (d, i) { this.innerHTML = data.finish; });
}
