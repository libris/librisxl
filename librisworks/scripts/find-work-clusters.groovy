/**
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import java.util.concurrent.ConcurrentHashMap

PrintWriter failedQueries = getReportWriter("failed-queries")
PrintWriter tooLargeResult = getReportWriter("too-large-result")

visited = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>())

def process = { bib ->
    if (!visited.add(bib.doc.shortId))
        return

    try {
        def instance = bib.graph[1]
        def work = loadIfLink(instance.instanceOf)

        if (!work) return

        def titles = [instance, work].grep().collect { title(it) }.grep().unique()

        Set ids = []

        titles.each {
            def q = buildQuery(work, it)
            if (!q) {
                return
            }
            ids.addAll(queryIds(q))
        }

        if (ids.size() > 1000) {
            tooLargeResult.println("Results: ${ids.size()} Id: ${bib.doc.shortId} Titles: ${titles}")
        } else if (ids.size() > 1) {
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

selectByCollection('bib') {
    process(it)
}

Map<String, List<String>> buildQuery(Map work, String title) {
    Map<String, List<String>> query = [
            "q"                 : ["*"],
            "@type"             : ["Instance"],
            "hasTitle.mainTitle": [esSafe(title)],
    ]

    insertLinkedAgents(work)
    def card = getWhelk().jsonld.toCard(work, false, true)

    def author = primaryContributor(card).collect { esSafe(it) }
    if (author) {
        query["or-instanceOf.contribution._str"] = author
        query["or-instanceOf.contribution.agent._str"] = author
        return query
    }

    def allContributors = contributors(card).collect { esSafe(it) }
    if (allContributors) {
        query["or-instanceOf.contribution._str"] = allContributors
        query["or-instanceOf.contribution.agent._str"] = allContributors
        return query
    }

    return null
}

private void insertLinkedAgents(work) {
    asList(work['contribution']).each {
        def agent = asList(it.agent).find()
        if (agent && agent['@id']) {
            it.agent = loadThing(agent['@id'])
        }
    }
}

private String title(Map thing) {
    return getAtPath(thing, ['hasTitle', 0, 'mainTitle'])
}

private List primaryContributor(work) {
    contributorStrings(asList(work['contribution']).find { it['@type'] == "PrimaryContribution" })
}

private List contributors(work) {
    asList(work['contribution']).collect { contributorStrings(it) }.grep().flatten()
}

//getAtPath(contribution, ['_str'])?.with { String s -> s.replaceAll(/[^ \p{IsAlphabetic}]/, '') }
private List contributorStrings(contribution) {
    List variants = asList(contribution?.agent) + asList(getAtPath(contribution, ['agent', 'hasVariant']))

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

private loadIfLink(Map work) {
    work?['@id'] ? loadThing(work['@id']) : work
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}