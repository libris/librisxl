/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

PrintWriter failedQueries = getReportWriter("failed-queries")
PrintWriter tooLargeResult = getReportWriter("too-large-result")

//def yesterday = new SimpleDateFormat('yyyy-MM-dd').with { sdf ->
//    Calendar.getInstance().with { c ->
//        c.add(Calendar.DATE, -1)
//        sdf.format(c.getTime())
//    }
//}

//def where = """
//    collection = '%s'
//    AND (modified::date = '$yesterday'
//        OR (data#>>'{@graph,0,generationDate}')::date = '$yesterday')
//"""

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())  // TODO: remove?
//instancesOfUpdatedLinkedWorks = Collections.synchronizedSet([] as Set)
//
//selectBySqlWhere(String.format(where, 'auth')) {
//    def thing = it.graph[1]
//    if (Normalizers.isInstanceOf(it.whelk.jsonld, thing, 'Work')) {
//        selectBySqlWhere("collection = 'bib' and data#>>'{@graph,1,instanceOf,@id}' = '${thing['@id']}'") {
//            instancesOfUpdatedLinkedWorks.add(it.doc.shortId)
//        }
//    }
//}

def process = { bib ->
    if (!visited.add(bib.doc.shortId))
        return

    try {
        def q = buildQuery(bib)
        if (!q) {
            return
        }

        List ids = queryIds(q).collect()

        if (ids.size() > 200) {
            tooLargeResult.println("Results: ${ids.size()} Query: ${q}")
        }
        else if (ids.size() > 1) {
            visited.addAll(ids)
            println(ids.join('\t'))
        }
    }
    catch (Exception e) {
        failedQueries.println("Error in ${bib.doc.shortId}: ${e}")
        e.printStackTrace()
        return
    }
}

//selectByIds(instancesOfUpdatedLinkedWorks) {
//    process(it)
//}

// TODO: Change when starting to run regularly
//selectBySqlWhere(String.format(where, 'bib')) {
selectByCollection('bib') {
    process(it)
}

Map<String, List<String>> buildQuery(bib) {
    def title = title(bib)

    if (!title)
        return null

    Map<String, List<String>> query = [
            "q"                 : ["*"],
            "@type"             : ["Instance"],
            "hasTitle.mainTitle": [esSafe(title)],
    ]

    insertLinkedAgents(bib)
    def card = bib.asCard(true)

    def author = primaryContributor(card).collect{ esSafe(it) }
    if (author) {
        query["or-instanceOf.contribution._str"] = author
        query["or-instanceOf.contribution.agent._str"] = author
        return query
    }

    def allContributors = contributors(card).collect{ esSafe(it) }
    if (allContributors) {
        query["or-instanceOf.contribution._str"] = allContributors
        query["or-instanceOf.contribution.agent._str"] = allContributors
        return query
    }
    return null
}

private void insertLinkedAgents(bib) {
    getPathSafe(bib.doc.data, ['@graph', 1, 'instanceOf', 'contribution']).each {
        def agent = asList(it.agent).find()
        if (agent && agent['@id']) {
            it.agent = loadThing(agent['@id'])
        }
    }
}

private String title(bib) {
    return getPathSafe(bib.doc.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
}

private List primaryContributor(bib) {
    contributorStrings(getPathSafe(bib, ['@graph', 1, 'instanceOf', 'contribution'], []).find { it['@type'] == "PrimaryContribution" })
}

private List contributors(bib) {
    getPathSafe(bib, ['@graph', 1, 'instanceOf', 'contribution'], []).collect { contributorStrings(it) }.grep().flatten()
}

//getPathSafe(contribution, ['_str'])?.with { String s -> s.replaceAll(/[^ \p{IsAlphabetic}]/, '') }
private List contributorStrings(contribution) {
    List variants = asList(contribution?.agent) + asList(getPathSafe(contribution, ['agent', 'hasVariant']))

    variants.grep().collect { name(it) }.grep()
}

private String name(Map agent) {
    agent.givenName && agent.familyName
            ? "${agent.givenName} ${agent.familyName}"
            : agent.name
}

// Remove ES query operators from string
private String esSafe(String s) {
    s.replaceAll('[+|"\\-*~]', " ")
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