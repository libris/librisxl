/**
 * Find clusters of records that may contain descriptions of the same work.
 * In short, similar descriptions are found by, for each bib record, querying Elastic for other records
 * having the same instance or work title and the same agent(s) in work contribution.
 * The ids found by the query become a cluster.
 * See script for more details.
 *
 * (When running, redirect STDERR to avoid annoying prints from whelktool)
 */

import static se.kb.libris.mergeworks.Util.AGENT
import static se.kb.libris.mergeworks.Util.HAS_TITLE
import static se.kb.libris.mergeworks.Util.MAIN_TITLE
import static se.kb.libris.mergeworks.Util.PRIMARY
import static se.kb.libris.mergeworks.Util.CONTRIBUTION
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY

PrintWriter failedQueries = getReportWriter("failed-queries")
PrintWriter tooLargeResult = getReportWriter("too-large-result")

def process = { bib ->
    try {
        def instance = bib.graph[1]
        def work = loadIfLink(instance.instanceOf)

        if (!work) return

        // Get mainTitle from both instance and work (we want to search for both when they differ)
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
            // Sort so that duplicate clusters can easily be identified
            println(ids.sort().join('\t'))
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

    // If there is a primary contributor, include only that in agent in the query
    def author = primaryContributor(card).collect { esSafe(it) }
    if (author) {
        query["or-instanceOf.contribution._str"] = author
        query["or-instanceOf.contribution.agent._str"] = author
        return query
    }

    // If no primary contributor, include all agents in the query
    def allContributors = contributors(card).collect { esSafe(it) }
    if (allContributors) {
        query["or-instanceOf.contribution._str"] = allContributors
        query["or-instanceOf.contribution.agent._str"] = allContributors
        return query
    }

    return null
}

private void insertLinkedAgents(work) {
    asList(work[CONTRIBUTION]).each {
        def agent = asList(it[AGENT]).find()
        if (agent && agent[ID_KEY]) {
            it.agent = loadThing(agent[ID_KEY])
        }
    }
}

private String title(Map thing) {
    return getAtPath(thing, [HAS_TITLE, 0, MAIN_TITLE])
}

private List primaryContributor(work) {
    contributorStrings(asList(work[CONTRIBUTION]).find { it[TYPE_KEY] == PRIMARY })
}

private List contributors(work) {
    asList(work[CONTRIBUTION]).collect { contributorStrings(it) }.grep().flatten()
}

//getAtPath(contribution, ['_str'])?.with { String s -> s.replaceAll(/[^ \p{IsAlphabetic}]/, '') }
private List contributorStrings(contribution) {
    List variants = asList(contribution?[AGENT]) + asList(getAtPath(contribution, [AGENT, 'hasVariant']))

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
    work?[ID_KEY] ? loadThing(work[ID_KEY]) : work
}

private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}