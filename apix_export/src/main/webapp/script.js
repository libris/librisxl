function loaded()
{
    setInterval(reload_console, 1000);
}

function reload_console()
{
    var httpRequest = new XMLHttpRequest();
    httpRequest.onreadystatechange = function()
    {
        if (httpRequest.readyState === XMLHttpRequest.DONE && httpRequest.status === 200)
        {
            document.getElementById("console").innerHTML = httpRequest.responseText;
        }
    };
    httpRequest.open("GET", "app/console", true);
    httpRequest.send(null);
}

window.onload = loaded;
