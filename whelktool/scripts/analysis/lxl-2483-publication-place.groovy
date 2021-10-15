import whelk.Document

prod = ['publication', 'production', 'manufacture']

selectByCollection('bib') { doc ->
    prod.each { p ->
        getPathSafe(doc.graph, [1, p], []).each {
            def place = asList(getPathSafe(it, ['place', 'label'])).flatten()
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