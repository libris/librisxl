function loaded()
{
    setInterval(reload_console, 1000);
    reload_string("apixurl", "app/endpoint");
    reload_string("xlurl", "app/startpoint");
}

function reload_string(elementid, url)
{
    var httpRequest = new XMLHttpRequest();
    httpRequest.onreadystatechange = function()
    {
        if (httpRequest.readyState === XMLHttpRequest.DONE && httpRequest.status === 200)
        {
            document.getElementById(elementid).innerHTML = httpRequest.responseText;
        }
    };
    httpRequest.open("GET", url, true);
    httpRequest.send(null);
}

function post(url, data)
{
    var httpRequest = new XMLHttpRequest();
    httpRequest.open("POST", url, true);
    httpRequest.send(data);
}

function reload_console()
{
    reload_string("console", "app/console");
}

window.onload = loaded;
