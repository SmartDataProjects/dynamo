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
