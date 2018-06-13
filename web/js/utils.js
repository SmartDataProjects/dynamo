function truncateText(textNode, width) {
    if (textNode.getBBox().width < width)
        return;

    var content = textNode.textContent;
    var truncateAt = content.length - 1;
    while (textNode.getBBox().width > width) {
        textNode.textContent = content.slice(0, truncateAt) + '...';
        truncateAt -= 1;
    }
}

function handleError(jqXHR, textStatus, errorThrown) {
  var msg = 'Error fetching data: ';
  switch (jqXHR.status) {
  case 400:
    msg += 'Bad HTTP request';
    break;
  case 403:
    msg += 'Permission denied';
    break;
  case 404:
    msg += 'Not found';
    break;
  case 500:
    msg += 'Internal server error';
    break;
  default:
    msg += 'Unknown error';
    break;
  }
  $('#error').html(msg);
}

// global constant
var dataPath = window.location.pathname.replace('web', 'data');

