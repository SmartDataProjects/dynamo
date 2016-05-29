function submit(caller, keyvals)
{
    for (var key in keyvals) {
        var elem = document.getElementById(key);
        if (elem != undefined) {
            elem.value = keyvals[key];
        }
    }
    var parent = caller.parentNode;
    while (parent.tagName.toLowerCase() != "form")
        parent = parent.parentNode;

    parent.submit();
}
