/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

clusterLog = getReportWriter("clusters.tsv")

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())

selectByCollection('bib') { bib ->
    if (!visited.add(bib.doc.shortId))
        return
    
    try {
        def q = buildQuery(bib)
        if (!q) {
            return
        }

        List ids = queryIds(q).collect()

        if (ids.size() > 1) {
            visited.addAll(ids)
            clusterLog.println(ids.join('\t'))
        }
    }
    catch (Exception e) {
        println(e)
        return
    }
}

exit()

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
    System.exit(0)
}

private String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
}

private String primaryContributorId(bib) {
    def primary = getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'contribution'], []).grep{ it['@type'] == "PrimaryContribution"}
    return getPathSafe(primary, [0, 'agent', '@id'])
}

private List contributorStrings(bib) {
    return getPathSafe(bib.asCard(true), ['@graph', 1, 'instanceOf', 'contribution'], [])['_str'].grep{it}
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
