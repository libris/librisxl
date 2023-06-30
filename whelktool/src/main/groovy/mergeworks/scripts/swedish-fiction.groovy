package mergeworks.scripts

import mergeworks.Doc

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) {cluster ->
    List<Doc> docs = Collections.synchronizedList([])
    selectByIds(cluster) {
        docs.add(new Doc(it))
    }

    def qualityMonographs = { Doc doc ->
        doc.isText()
                && doc.isMonograph()
                && !doc.isManuscript()
                && !doc.isMaybeAggregate()
                && doc.encodingLevel() != 'marc:PartialPreliminaryLevel'
                && doc.encodingLevel() != 'marc:PrepublicationLevel'
                && !doc.isTactile()
                && !doc.isDrama()
                && !doc.isThesis()
                && !doc.isInSb17Bibliography()
    }

    def swedish = { Doc doc ->
        asList(doc.workData['language']).collect { it['@id'] } == ['https://id.kb.se/language/swe']
    }

    def filtered = docs.split { it.instanceData }
            .with { local, linked ->
                linked + local.findAll(swedish).findAll(qualityMonographs)
            }

    if (filtered.size() > 1 && filtered.any { Doc d -> d.isFiction() } && !filtered.any { Doc d -> d.isNotFiction() }) {
        println(filtered.collect { Doc d -> d.shortId() }.join('\t'))
    }
}