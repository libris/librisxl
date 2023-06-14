import whelk.Document

errors = getReportWriter("errors.txt")

prod = ['publication', 'production', 'manufacture']

selectByCollection('bib') { doc ->
    try {
        process(doc)
    }
    catch (Exception e) {
        def m = "${doc.doc.shortId} $e"
        println(m)
        errors.println(m)
    }
}

void process(doc) {
    prod.each { p ->
        getPathSafe(doc.graph, [1, p], []).each {
            def place = asList(getPathSafe(it, ['place', 'label'])).flatten().join(' | ')
            if (place) {
                incrementStats(p, place)
            }
        }
    }
}

Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}