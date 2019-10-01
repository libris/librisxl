/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())
clusters = Collections.newSetFromMap(new ConcurrentHashMap<Set<String>, Boolean>())

selectByCollection('bib') { bib ->
    if (!visited.add(bib.doc.shortId))
        return

    if (visited.size() >= 10_000) {
        exit()
    }

    try {
        title = title(bib)
        authorId = primaryContributorId(bib)

        if (!title || !authorId)
            return

        Map<String, List<String>> query = [
                "q"                                : ["*"],
                "@type"                            : ["*"],
                "hasTitle.mainTitle"               : [title + "~"],
                //"_str"                             : [title + "~"],
                "instanceOf.contribution.agent.@id": [authorId]
        ]

        def docs = queryDocs(query).collect()
        def ids = docs.collect{it['_id']}

        if (ids.size() > 1) {
            visited.addAll(ids)
            clusters.add(ids)
            log(bib, query, docs, ids)
        }
    }
    catch (Exception e) {
        println(e)
        return
    }
}

void exit() {
    def tot = clusters.sum({ it.size() })
    def avg = tot / clusters.size()
    clusters = clusters.sort{ it.size() }
    def median = clusters[clusters.size()/2].size()
    def max = clusters.last()
    def percent = 100 * tot / visited.size()

    println(String.format("bibs:%s clusters:%s, tot:%s (%.2f%%), median size:%s, avg size:%.2f, max size:%s (%s)",
            visited.size(), clusters.size(), tot, percent, median, avg, max.size(), max[0])
    )

    System.exit(0)
}

void log(bib, query, docs, ids) {
    StringBuilder b = new StringBuilder()
    b.append(flatTitle(bib)).append(": ").append(query).append("\n")
    b.append(bib.doc.shortId).append(" ----> ").append(ids.toString()).append("\n")

    List l = Collections.synchronizedList([])
    selectByIds(ids) { bib2 ->
        l.add(new Tuple2(bib2.doc.getURI().toString(), flatTitle(bib2)))
    }
    l.sort{ it.second }.each{ b.append(it.first).append(" :\t").append(it.second).append("\n") }

    docs.collect{ d-> new Tuple2(d.'_id', d.'_str') }
            .sort{ it.second }
            .each{ b.append(it.first).append(" _str:\t").append(it.second).append("\n") }

    b.append("\n")
    println(b.toString())
}

String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
}

String primaryContributorId(bib) {
    def primary = getPathSafe(bib.doc.data, ['@graph', 2, 'contribution'], []).grep{ it['@type'] == "PrimaryContribution"}
    return getPathSafe(primary, [0, 'agent', '@id'])
}

private String flatTitle(bib) {
    return flatten(
            bib.doc.data['@graph'][1]['hasTitle'],
            ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName', ]
    )
}

private String flatten(Object o, List order) {
    if (o instanceof String) {
        return o
    }
    if (o instanceof List) {
        return o
                .collect{ flatten(it, order) }
                .join(' || ')
    }
    if (o instanceof Map) {
        return order
                .collect{ o.get(it, null) }
                .grep{ it != null }
                .collect{ flatten(it, order) }
                .join(' | ')
    }

    throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
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
