selectByCollection('bib') { bib ->
    def resp = getPathSafe(bib.doc.data, ['@graph', 1, 'responsibilityStatement'])
    if (resp) {
        println(resp)
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
