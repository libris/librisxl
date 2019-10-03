/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

html = getReportWriter("someworks.html")

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())
clusters = Collections.newSetFromMap(new ConcurrentHashMap<Set<String>, Boolean>())

html.println('<html>')

selectByCollection('bib') { bib ->
    if (!visited.add(bib.doc.shortId))
        return

    if (visited.size() >= 100_000) {
        exit()
    }

    try {
        def q = buildQuery(bib)
        if (!q) {
            return
        }

        def docs = queryDocs(q).collect()
        def ids = docs.collect{it['_id']}

        if (ids.size() > 1) {
            visited.addAll(ids)
            clusters.add(ids)
            logHtml(bib, q, docs, ids)
        }
    }
    catch (Exception e) {
        println(e)
        return
    }
}

Map<String, List<String>> buildQuery(bib) {
    def title = title(bib)

    if (!title)
        return null

    Map<String, List<String>> query = [
            "q"                                : ["*"],
            "@type"                            : ["*"],
            "hasTitle.mainTitle"               : [title + "~"],
    ]

    def author = primaryContributorId(bib)
    if (author) {
        query["instanceOf.contribution.agent.@id"] = [author]
        return query
    }

    def contributors = contributorStrings(bib)
    if (contributors) {
        query["instanceOf.contribution._str"] = contributors.collect{ it + "~" }
        return query
    }

    return null
}

synchronized void exit() {
    def tot = clusters.sum({ it.size() })
    def avg = tot / clusters.size()
    clusters = clusters.sort{ it.size() }
    def median = clusters[clusters.size()/2].size()
    def max = clusters.last()
    def percent = 100 * tot / visited.size()

    println(String.format("bibs:%s clusters:%s, tot:%s (%.2f%%), median size:%s, avg size:%.2f, max size:%s (%s)",
            visited.size(), clusters.size(), tot, percent, median, avg, max.size(), max[0])
    )

    html.println('</html>')

    System.exit(0)
}

private String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
}

private String primaryContributorId(bib) {
    def primary = getPathSafe(bib.doc.data, ['@graph', 2, 'contribution'], []).grep{ it['@type'] == "PrimaryContribution"}
    return getPathSafe(primary, [0, 'agent', '@id'])
}

private List contributorStrings(bib) {
    return getPathSafe(bib.asCard(true), ['@graph',2,'contribution'], [])['_str'].grep{it}
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

void logHtml(bib, query, docs, ids) {
    def m = new ConcurrentHashMap<String, Map>()
    selectByIds(ids) { bib2 ->
        def w =["id": bib2.doc.shortId, "uri": bib2.doc.getURI().toString(), "title": flatTitle(bib2), "contrib": contributorStrings(bib)]
        m.put(bib2.doc.shortId, w)
    }

    docs.each { d ->
        m[d.'_id'].'_str' = d.'_str'
    }

    def StringBuilder h = new StringBuilder()
    h.append('<b>').append(flatTitle(bib)).append('</b><br>\n')
    h.append("<i>").append(query).append('</i><br>\n')
    h.append(bib.doc.shortId).append(" ----> ").append(ids.toString()).append("<br>\n")
    h.append('<table>\n')

    m.values().sort{it['title']}.each { w ->
        h.append('<tr><td><a href="').append(w.'uri',).append('">').append(w.'title').append('</td><td>')
                .append(w.'contrib').append('</td><td>')
                .append(w.'_str').append('</td><td>').append(w.'_str').append('</td></tr>')append('\n')
    }
    h.append('</table><br><br>\n')

    html.println(h.toString())
    println(h.toString())
}