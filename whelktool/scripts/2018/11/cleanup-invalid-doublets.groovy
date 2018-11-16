leftovers = getReportWriter("titledoublet-leftovers")

modifiedLog = System.properties.logModified == 'true' ? getReportWriter("doublets-modified") : null
currentData = null
synchronized void logModified(data, Closure cb) {
    if (!modifiedLog) return
    if (currentData != data) modifiedLog.println "<${data.graph[0][ID]}>:"
    cb(modifiedLog)
    currentData = data
}

void fixTitles(data, resource) {
    List<Map> titles = resource.hasTitle

    if (!titles instanceof List)
        return

    def regularTitles = titles.findAll { it[TYPE] == 'Title' }

    if (!regularTitles || regularTitles.size() == 1)
        return

    def bestTitle = (regularTitles.findAll {
            it.mainTitle && 'subtitle' in it
        } ?: regularTitles.findAll {
            it.mainTitle && !it.mainTitle.matches('.*[=:/.]\\s*$')
        } ?: regularTitles)[-1]

    int titlesSize = titles.size()

    titles.removeAll { it in regularTitles && !it.is(bestTitle) }

    if (titlesSize != titles.size()) {
        data.scheduleSave(loud: false)
        logModified(data) { log ->
            regularTitles.each {
                def kept = it.is(bestTitle) ? 'KEEP' : 'DROP'
                log.println "  ${kept}\t${it}"
            }
        }
    } else {
        leftovers.println "<${data.graph[0][ID]}> titles: ${titles.size()}"
    }
}

boolean similarContribution(Map contrib, Map toKeep) {
    if (contrib.agent?.get(ID)) {
        // TODO: fetch and compare...
        if (toKeep.agent?.get(ID) == contrib.agent[ID]) {
            return true
        }
        def agent = load(contrib.agent[ID])[GRAPH][1]
        if (similarEntity(agent, toKeep.agent)) {
            toKeep.agent[ID] = contrib.agent[ID]
            return true
        } else {
            return false
        }
    }
    return similarEntity(contrib.agent, toKeep.agent)
}

boolean overlaps(Set a, Set b, double minOverlap=0.6) {
    return (a.intersect(b).size() /
            Integer.max(a.size(), b.size())
            ) >= minOverlap
}

boolean similarEntity(Map a, Map b) {
    if (a == null || b == null) {
        return a == b
    }
    if (!overlaps(a.keySet(), b.keySet())) {
        return false
    }
    def nonSimilar = a?.find { k, av ->
        def bv = b[k]
        av instanceof String && bv instanceof String &&
        !overlaps(av.toLowerCase() as Set, bv.toLowerCase() as Set)
    }
    return nonSimilar == null
}

/**
 * Fixes List-wrapped agent on a contribution.
 * @return Whether the agent object was fixed or not.
 */
boolean fixAgentObject(contrib) {
    def agent = contrib.agent
    if (agent instanceof List) {
        if (agent.size() == 1) {
            contrib.agent = agent[0]
            return true
        }
        return false
    }
    return true
}

void fixPrimaryContribs(data, resource) {
    List<Map> contribs = resource.contribution
    if (!(contribs instanceof List))
        return
    if (contribs.size() < 2)
        return

    // Tentatively adjust single-agents-in-lists (won't be saved unless any
    // doublet is fixed)
    contribs.each {
        fixAgentObject(it)
    }

    int contribsSize = contribs.size()

    def primaryContribs = contribs.findAll { it[TYPE] == 'PrimaryContribution' }
    if (!primaryContribs || primaryContribs.size() == 1)
        return

    Map keptPrimaryContrib = primaryContribs[-1]

    def unwantedPrimaryContribs = primaryContribs[0..-2] as Set<Map>
    unwantedPrimaryContribs.removeAll {
        if (!similarContribution(it, keptPrimaryContrib)) {
            it[TYPE] = 'Contribution'
            data.scheduleSave(loud: false)
            return true
        } else {
            // TODO: if similar, assign linked role to keptPrimaryContrib?
            it.role?.each {
                if (ID in it) {
                    if (keptPrimaryContrib.role instanceof List &&
                        !keptPrimaryContrib.role.any {ID in it}) {
                        keptPrimaryContrib.role << it
                    } else {
                        keptPrimaryContrib.role = [it]
                    }
                }
            }
            return false
        }
    }

    contribs.sort { it[TYPE] != 'PrimaryContribution' }

    if (unwantedPrimaryContribs &&
            contribs.removeAll {
                !it.is(keptPrimaryContrib) &&
                it in unwantedPrimaryContribs
            }) {
        if (contribs.size() > 1) {
            Set seenContribs = new HashSet()
            contribs.removeAll {
                boolean justAdded = seenContribs.add(it)
                it[TYPE] == 'Contribution' &&
                    ((!(it.role) &&
                      similarContribution(it, keptPrimaryContrib)) ||
                     !justAdded)
            }
        }
        def keptAgentId = keptPrimaryContrib.agent?.get(ID)
        if (keptAgentId) {
            keptPrimaryContrib.agent.clear()
            keptPrimaryContrib.agent[ID] = keptAgentId
        }
        data.scheduleSave(loud: false)
        logModified(data) { log ->
            log.println "  DROP\t${contribsSize - contribs.size()} contribution objects (${contribs.size()} left)"
        }
    }
}

//selectByIds(['fzr7zx6r3ns1wc1', '1kcq4vsc3n3kggx', 'cwp4dbhp175mlkm', 'j2v9qsmv0vt1w5k', 'sb4g0lr43fcrz42', 'cvn87b7p1zmsjhf', '4ngs6vng0469nsm', 'gqtb6190dcdg0ngd', 'x6b9v2l2vg7bjll7', 'sb459gg42gx6dbx']) { data ->
selectBySqlWhere('''
    data#>>'{@graph,1,hasTitle}' LIKE '%"Title"%"Title"%'
    OR
    data#>>'{@graph,2,contribution}' LIKE '%"PrimaryContribution"%"PrimaryContribution"%'
''') { data ->
    def (record, instance, work) = data.graph
    if (!isInstanceOf(instance, 'Instance'))
        return

    if (!record.encodingLevel in ['marc:PrepublicationLevel',
            'marc:PartialPreliminaryLevel']) {
        return // TODO: we might still have dup data left ...
    }

    fixTitles(data, instance)

    if (work && isInstanceOf(work, 'Work')) {
        fixPrimaryContribs(data, work)
    }
}
