import whelk.util.DocumentUtil

PrintWriter removed = getReportWriter('removed.tsv')

selectByCollection('bib') { data ->
    Map instance = data.graph[1]
    String id = data.doc.shortId

    List badPaths = []

    // Remove broken links
    boolean modified = DocumentUtil.traverse(instance) { value, path ->
        if (value in String && value.startsWith('http://www.scb.se/')) {
            int respCode = getResponseCode(value)
            if (respCode in [400, 404]) {
                badPaths << path
                removed.println("${id}\t${respCode}\t${path}\t${value}")
                return new DocumentUtil.Remove()
            }
        }
    }

    // Remove the objects containing broken links altogether
    badPaths.reverseEach { bp ->
        if (instance[bp[0]] in List) {
            Map removedObj = instance[bp[0]].remove(bp[1])
            removedObj.each {
                incrementStats("removed", it)
            }
            if (instance[bp[0]].isEmpty())
                instance.remove(bp[0])
        }
        else if (instance[bp[0]] in Map) {
            Map removedObj = instance.remove(bp[0])
            removedObj.each {
                incrementStats("removed", it)
            }
        }
    }

    if (modified)
        data.scheduleSave()
}

int getResponseCode(String url) {
    try {
        URLConnection conn = new URL(url).openConnection()
        if (conn.getResponseCode() == 301)
            conn = new URL(conn.getHeaderField('Location')).openConnection()
        return conn.getResponseCode()
    } catch (Exception ex) {
        println(ex)
        return 0
    }
}