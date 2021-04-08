
def q = [
        '@type'                                            : ['Instance'],
        'exists-instanceOf.expressionOf.hasTitle.mainTitle': ['true'],
        'exists-instanceOf.expressionOf.@id'               : ['false'],
]

selectByIds(queryIds(q).collect()) { bib -> 
    List<Map> expressionOf = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'expressionOf']))

    if (!expressionOf) {
        return
    }
    
    expressionOf.each { e -> 
        if (e['@id']) {
            return
        }

        def expressionLang = asList(e['language']) as Set
        def workLang = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'language'])) as Set
        if (expressionLang != workLang) {
            //incrementStats(workLang.toString(), expressionLang.toString())
        }
        
        def contribution = asList(getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'contribution']))
        if (contribution) {
            e['contribution'] = contribution
        }
        
        incrementStats('shape', e.keySet())
        
        println(e)
    }
}

/*
def q = [
        'inCollection.@id': ['https://id.kb.se/term/uniformWorkTitle'],
]

selectByIds(queryIds(q).collect()) { bib ->
    Map work = getPathSafe(bib.doc.data, ['@graph', 1])

    work.remove('inCollection')
    println(work)
    incrementStats('shape', work.keySet())
}
*/
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

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
