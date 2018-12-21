import org.codehaus.jackson.map.ObjectMapper

def jsonWriter = new ObjectMapper().writer()

report = getReportWriter("work-report.txt")

Map loadObject(obj) {
    if (obj instanceof List) obj = obj[0]
    if (obj?.get(ID)) {
        obj = load(obj[ID])?.get(GRAPH)[1]
    }
    return obj
}

Map digestAgent(agent) {
    if (!agent)
        return null

    def name = [agent.givenName, agent.familyName].findResults { it?.trim() ?: null }.join(' ') ?: agent.name
    if (!name && agent['marc:subordinateUnit'] instanceof List)
        name = (agent['marc:subordinateUnit'] + [agent.isPartOf?.name]).findAll().join(' ')

    def id = agent.get(ID)?.replaceFirst('https://libris[^.]*.kb.se/', '')

    def obj = [:]
    if (id)
        obj.id = id
    obj.name = name
    if (agent.lifeSpan && agent.lifeSpan != '-')
        obj.bYear = agent.lifeSpan.split('-')[0]

    return obj
}

String makeRoleKey(role) {
    role?.getAt(0)?.get(ID)?.replaceFirst('https://id.kb.se/relator/', '')
}

/*
selectByIds([
    '5ng24q9h15s5pjj', '4db5v63f28vsx7nq', '0h9w2g2b3cf584l', '5ngdnv6h1d9wgw2', 'xf7vq5380zcp50n',
]) { data ->
*/
selectBySqlWhere('''
    data#>>'{@graph,1,issuanceType}' = 'Monograph'
    AND
    data#>>'{@graph,2,@type}' = 'Text'
    AND
    data#>>'{@graph,2,language,0,@id}' = 'https://id.kb.se/language/swe'
''') { data ->
    def (record, instance, work) = data.graph

    if (!isInstanceOf(instance, 'Instance')) return

    if (!instance.hasTitle?.getAt(0)?.mainTitle) return

    def primary = work.contribution.find { it[TYPE] == 'PrimaryContribution' }
    if (!primary) return

    def creator = loadObject(primary.agent)
    if (!creator) return

    // TODO: filtrera bort även de med originalVersion? Nej, vissa har inte denna angiven (jmf. dnkl44n4btxt1bms och p60vw7210hbjskn); då missar vi en del.

    // TODO: these might be primary in another description...
    // - also, some roles (e.g. abridger) is a strong indication that this is some kind of variant work
    def contributions = work.contribution.findAll { it[TYPE] != 'PrimaryContribution' }

    def publ = instance.publication.find { it[TYPE] == 'PrimaryPublication' }

    def creators = []
    creators += [digestAgent(creator)] +
                      (creator.hasVariant?.findResults { digestAgent(it) } ?: [])

    def contribs = contributions.findResults {
            def obj = digestAgent(loadObject(it.agent))
            if (!obj?.name)
                return null
            def role = makeRoleKey(it.role)
            if (role)
                obj.role = role
            return obj
        }

    def props = [
        hasTitle: instance.hasTitle,
        creator: creators,
        instanceType: instance[TYPE],
        publYear: publ?.year,
        encLevel: record.encodingLevel,
        record: record[ID],
        genreForm: work.genreForm?.findResults { it[ID] ?: null }.collect { it.replaceFirst('https://id.kb.se/', '') },
        contribs: contribs,
    ]
    if (work.hasTitle) {
        props.instanceOf = [hasTitle: work.hasTitle]
        if (work.expressionOf?.hasTitle) {
            props.instanceOf.expressionOf = [hasTitle: work.expressionOf.hasTitle]
        }
    }


    synchronized (report) {
        report.println jsonWriter.writeValueAsString(props)
    }
}
