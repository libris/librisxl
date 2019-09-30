import java.util.concurrent.ConcurrentHashMap

Set<String> visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())
Set<Set<String>> clusters = Collections.newSetFromMap(new ConcurrentHashMap<Set<String>, Boolean>())

selectByCollection('bib') {
    bib = it

    if (!visited.add(bib.doc.shortId))
        return

    try {
        title = title(bib)
        authorId = authorId(bib)

        if (!title || !authorId)
            return

        Map<String, List<String>> query = [
                "q"                                : ["*"],
                "@type"                            : ["*"],
                "hasTitle.mainTitle"               : [title + "~"],
                "instanceOf.contribution.agent.@id": [authorId]
        ]

        def ids = queryIds(query).collect()

        if (ids.size() > 1) {
            visited.addAll(ids)
            clusters.add(ids)

            println()
            println(query)
            println ("" + bib.doc.getURI() + " ---> " +ids)

            ids.remove(bib.doc.shortId)
            selectByIds(ids) { bib ->
                println("" + bib.doc.getURI() + " : " + title(bib))
            }
        }
    }
    catch (Exception e) {
        println(e)
        return
    }
}

String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph',1,'hasTitle','mainTitle',0])
}

String authorId(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 2, 'contribution', 0, 'agent', '@id'])
}

private String getPathSafe(item, path) {
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return null
        }
    }
    return item.toString()
}
