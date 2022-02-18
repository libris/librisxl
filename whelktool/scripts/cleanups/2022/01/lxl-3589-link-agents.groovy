import whelk.util.DocumentUtil
import whelk.Document
import whelk.Whelk

ambiguous = getReportWriter('ambiguous.txt')
linked = getReportWriter('linked.txt')

agentTypes =
        [
                'Family',
                'Jurisdiction',
                'Organization',
                'Meeting',
                'Person'
        ] as Set

agents = loadAgents()

selectByCollection('bib') { data ->
    def thing = data.graph[1]
    def id = data.doc.shortId
    def work = thing.instanceOf

    if (!work || work in List || work.'@id')
        return

    boolean modified = false

    asList(work.contribution).each { c ->
        asList(c.agent).each { a ->
            modified |= tryLinkAndReport(a, id)
        }
    }

    asList(work.subject).each { s ->
        if (s.'@type' == 'ComplexSubject') {
            s.termComponentList.each { tc ->
                modified |= tryLinkAndReport(tc, id)
            }
        } else {
            modified |= tryLinkAndReport(s, id)
        }
    }

    if (modified)
        data.scheduleSave()
}

boolean tryLinkAndReport(Map agent, String id) {
    def type = agent.'@type'

    if (type in agentTypes) {
        def copy = Document.deepCopy(agent)
        normalize(copy)
        def linkable = agents[copy]

        if (linkable?.size() == 1) {
            linked.println("$id\t${agent}\t${getShortId(linkable[0])}")
            incrementStats("Linked $type", orderKeys(agent) - '@type')
            agent.clear()
            agent['@id'] = linkable[0]
            return true
        } else if (linkable?.size() > 1) {
            ambiguous.println("$id\t${agent}\t${linkable.collect { getShortId(it) }}")
        }

        incrementStats("Not linked $type", orderKeys(agent) - '@type')
    }

    return false
}

Map loadAgents() {
    def agents = Collections.synchronizedMap([:])
    def whelk = Whelk.createLoadedCoreWhelk()

    selectByCollection('auth') {
        def agent = it.graph[1]
        def type = agent.'@type'
        def id = agent.'@id'

        if (type in agentTypes) {
            def chip = whelk.jsonld.toChip(agent)
            chip.remove('@id')
            chip.remove('sameAs')
            normalize(chip)
            if (agents.containsKey(chip))
                agents[chip] << id
            else
                agents[chip] = [id] as Set
        }
    }

    return agents
}

void normalize(Map agent) {
    DocumentUtil.traverse(agent) { value, path ->
        if (value in String) {
            def normValue = path.last() in Integer ? removeTrailingPeriod(value) : asList(removeTrailingPeriod(value))
            return new DocumentUtil.Replace(normValue)
        }
    }
}

String removeTrailingPeriod(String s) {
    return s.replaceFirst(/\.$/, '')
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}

List orderKeys(Map agent) {
    return agent.collect { it.key }.sort()
}

String getShortId(String id) {
    return id.split('/|#')[-2]
}
