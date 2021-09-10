/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

clusterLog = getReportWriter("clusters.tsv")

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())  // TODO: remove?

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
        e.printStackTrace()
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
            "@type"                            : ["Instance"],
            "hasTitle.mainTitle"               : [title + "~"],
    ]

    insertLinkedAgents(bib)
    def card = bib.asCard(true)
    
    def author = primaryContributor(card) //.collect{ it + "~" }
    if (author) {
        query["or-instanceOf.contribution._str"] = author
        query["or-instanceOf.contribution.agent._str"] = author
        return query
    }

    def allContributors = contributors(card) //.collect{ it + "~" }
    if (allContributors) {
        query["or-instanceOf.contribution._str"] = allContributors
        query["or-instanceOf.contribution.agent._str"] = allContributors
        return query
    }
    return null
}

synchronized void exit() {
    System.exit(0)
}

private void insertLinkedAgents(bib) {
    getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'contribution']).each {
        if (it.agent && it.agent['@id']) {
            it.agent = loadThing(it.agent['@id'])
        }
    }
}

private String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
}

private List primaryContributor(bib) {
    contributorStrings(getPathSafe(bib, ['@graph', 1, 'instanceOf', 'contribution'], []).find{ it['@type'] == "PrimaryContribution"})
}

private List contributors(bib) {
    getPathSafe(bib, ['@graph', 1, 'instanceOf', 'contribution'], []).collect{ contributorStrings(it) }.grep().flatten()
}

//getPathSafe(contribution, ['_str'])?.with { String s -> s.replaceAll(/[^ \p{IsAlphabetic}]/, '') }
private List contributorStrings(contribution) {
    List variants = asList(contribution?.agent) + asList(getPathSafe(contribution, ['agent', 'hasVariant']))

    variants.collect { name(it) }.grep()
}

private String name(Map agent)  {
    agent.givenName && agent.familyName 
            ? "${agent.givenName} ${agent.familyName}"
            : agent.name
}

private Object getPathSafe(item, path, defaultTo = null) {
    if (!item) {
        return defaultTo
    }
    
    for (p in path) {
        if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

private static List asList(Object o) {
    (o ?: []).with { it instanceof List ? it : [it] }
}