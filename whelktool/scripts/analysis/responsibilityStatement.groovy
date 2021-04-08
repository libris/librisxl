selectByCollection('bib') { bib ->
    def (record, thing) = bib.graph
    if (thing.responsibilityStatement) {
        println(bib.doc.shortId + ' ' + thing.responsibilityStatement)
    }
}

private Object getPathSafe(item, path, defaultTo = null) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}
