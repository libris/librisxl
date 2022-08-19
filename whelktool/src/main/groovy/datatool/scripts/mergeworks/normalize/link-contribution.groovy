package datatool.scripts.mergeworks.normalize

import groovy.transform.Memoized
import whelk.Document

import static datatool.scripts.mergeworks.Util.asList
import static datatool.scripts.mergeworks.Util.chipString
import static datatool.scripts.mergeworks.Util.getPathSafe
import static datatool.scripts.mergeworks.Util.nameMatch
import static datatool.scripts.mergeworks.Util.Relator

/**
 Example:
 $ ENV=qa && time java -Dxl.secret.properties=$HOME/secret.properties-$ENV -Dclusters="reports/clusters.tsv" -jar build/libs/whelktool.jar --report reports/$ENV-$(date +%Y%m%d-%H%M%S) --dry-run src/main/groovy/datatool/scripts/mergeworks/normalize/link-contribution.groovy
 */

PrintWriter report = getReportWriter("report.txt")

def whelk = getWhelk()

def contributionPath = ['@graph', 1, 'instanceOf', 'contribution']

new File(System.getProperty('clusters')).splitEachLine('\t') { cluster ->
    def docs = Collections.synchronizedList([])
    selectByIds(cluster.collect { it.trim() }) {
        docs << it.doc
    }

    List<Map> linked = []
    List<Map> primaryAutAgents = []
    docs.each { Document d ->
        def contribution = getPathSafe(d.data, contributionPath, [])
        contribution.each { Map c ->
            if (c.agent && c.agent['@id']) {
                loadThing(c.agent['@id'])?.with { Map agent ->
                    agent.roles = asList(c.role)
                    linked << agent
                }
            }
            if (c['@type'] == 'PrimaryContribution' && c['role'] == ['@id': 'https://id.kb.se/relator/author'] && c['agent']) {
                Map agent = loadIfLink(c['agent'])
                if (agent) {
                    primaryAutAgents << agent
                }
            }
        }
        incrementStats('link contribution', 'docs checked')
    }

    docs.each { Document d ->
        def changed = false

        def contribution = getPathSafe(d.data, contributionPath, [])
        contribution.each { Map c ->
            if (c.agent && !c.agent['@id']) {
                def l = linked.find {
                    agentMatches(c.agent, it) && (!c.role || it.roles.containsAll(c.role))
                }
                if (l) {
                    report.println("${d.shortId} ${chipString(c, whelk)} --> ${chipString(l, whelk)}")
                    c.agent = ['@id': l['@id']]
                    changed = true
                    incrementStats('link contribution', 'agents linked')
                } else {
                    report.println("${d.shortId} NO MATCH: ${chipString(c, whelk)} ??? ${linked.collect { chipString(it, whelk) }}")
                }
            }
            if (c['@type'] == 'PrimaryContribution' && !c.role) {
                if (c.agent) {
                    def agent = loadIfLink(c.agent)
                    if (primaryAutAgents.any { agentMatches(agent, it) }) {
                        c.role = ['@id': Relator.AUTHOR.iri]
                        changed = true
                        incrementStats('link contribution', 'author role added to primary contribution')
                    }
                }
            }
        }
        if (changed) {
            selectByIds([d.shortId]) {
                it.doc.data = d.data
                it.scheduleSave()
            }
        }
    }
}

private Map loadIfLink(Map m) {
    m['@id'] ? loadThing(m['@id']) : m
}

@Memoized
private Map loadThing(def id) {
    def thing = [:]
    selectByIds([id]) { t ->
        thing = t.graph[1]
    }
    return thing
}

static boolean agentMatches(Map local, Map linked) {
    nameMatch(local, linked) && !yearMismatch(local, linked)
}

static boolean yearMismatch(Map local, Map linked) {
    def birth = { Map p -> p.lifeSpan?.with { (it.replaceAll(/[^\-0-9]/, '').split('-') as List)[0] } }
    def death = { Map p -> p.lifeSpan?.with { (it.replaceAll(/[^\-0-9]/, '').split('-') as List)[1] } }
    def b = birth(local) && birth(linked) && birth(local) != birth(linked)
    def d = death(local) && death(linked) && death(local) != death(linked)
    b || d
}

def getWhelk() {
    // A little hack to get a handle to whelk...
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}


