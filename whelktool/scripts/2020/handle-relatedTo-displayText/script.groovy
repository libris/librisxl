import whelk.datatool.DocumentItem


RELATED_TO = 'relatedTo'
DISPLAY_TEXT = 'marc:displayText'
LINK_NOTE = 'Note' // marc:LinkingEntryComplexityNote ?:D

boolean dropDisplayText(displayText) {
    return displayText =~ /^projekt$|^värdpub.*|^channel ?record/
}

MATCH_REVIEW_OF = /r.?e.?[czv]\s?e.*i.?(on|rt)\w*\W+(av|of)/

MATCH_COMMENT = /^komment.+(till|kring|på)/

MATCH_CONTINUED_BY = /^forts.*/

MATCH_SUPPLEMENT_TO = /^bilaga.+(till|kring|på)/

MATCH_PARALLELL = /^parall?ell?|^repr(int|oduct).*/

HANDDRAWN_GENREFORM = [
    "https://id.kb.se/marc/Drawing",
    "https://id.kb.se/term/gmgpc%2F%2Fswe/Teckningar",
    "https://id.kb.se/term/gmgpc%2F%2Fswe/Blyertsteckningar"
] as Set

selectBySqlWhere("""
    data#>>'{@graph,2,${RELATED_TO}}' LIKE '%"${DISPLAY_TEXT}":%'
    AND collection = 'bib'
""") { DocumentItem data ->
    def (record, instance, work) = data.graph
    if (!work || work.size() == 1) {
        // TODO: missing or linked; OK to move along?
        return
    }

    Closure save = data.&scheduleSave

    def workLinks = [
        'hasDerivative', 'derivativeOf', 'reviewOf', 'relationship',
        'hasNote'
    ].collectEntries { [it, []] }

    def instanceLinks = [
        'otherEdition', 'continuedBy', 'supplementTo', 'relationship',
        'hasNote'
    ].collectEntries { [it, []] }

    def iter = work[RELATED_TO].iterator()
    for (node in iter) {
        def displayText = node.remove(DISPLAY_TEXT)
        if (displayText == null)
            continue

        if (displayText instanceof List) {
            displayText = displayText.join(' ')
        }

        String displaytxt = displayText.toLowerCase()
            .replaceAll(/[]\[]/, '').trim()

        node.remove('marc:toDisplayNote')

        if (dropDisplayText(displaytxt)) {
            node.remove(DISPLAY_TEXT)
            save()
        } else if (displaytxt =~ /förlaga till/)  {
            /*
            förlaga till

            förmodad handritad förlaga till
            handritad förlaga till
            handritad spegelvänd förlaga till
            ofullbordad handritad förlaga till
            sannolik förlaga till
            spegelvänd handritad förlaga till

            förlaga till detalj i
            handritad förlaga till detalj i
            handritad förlaga till detaljer i
            handritad förlaga till detaljer i
            förlaga till titelsidan i
            handritad förlaga till illustration i

            handskriven förlaga till
            handskriven förlaga till detalj i
            */
            if (
                displaytxt =~ /^förlaga till$/
                || (displaytxt =~ /handritad/ &&
                    work.genreForm?.any { it[ID] in HANDDRAWN_GENREFORM })
                || (displaytxt =~ /handskriven/ &&
                    instance[TYPE] == 'Manuscript')
            ) {
                iter.remove()
                workLinks.hasDerivative << node
            }
        } else if (displaytxt =~ /förlaga/)  {
            /*
            fransk förlaga
            förlaga
            förmodad handritad förlaga
            [handritad förlaga]
            handritad förlaga
            handritad förlaga (digital version)
            handritad förlaga (digitaliserad version)
            handskriven förlaga
            sannolik förlaga
            tryckt förlaga
            */
            iter.remove()
            workLinks.derivativeOf << node
        } else if (displaytxt =~ MATCH_REVIEW_OF) {
            iter.remove()
            workLinks.reviewOf << node
        } else if (displaytxt =~ MATCH_CONTINUED_BY) {
            iter.remove()
            instanceLinks.continuedBy << node
        } else if (displaytxt =~ MATCH_COMMENT) {
            String prettyDisplayText = displayText.replaceAll(/till\s*:/, '')
            workLinks.hasNote << [[TYPE]: LINK_NOTE, label: prettyDisplayText]
            save()
        } else if (displaytxt =~ MATCH_SUPPLEMENT_TO) {
            iter.remove()
            workLinks.supplementTo << node
        } else if (displaytxt =~ MATCH_PARALLELL) {
            iter.remove()
            instanceLinks.otherEdition << node
        } else {
            iter.remove()
            def leafInstance = node.remove('hasInstance')
            if (leafInstance instanceof List) {
                assert leafInstance.size() == 1
                leafInstance = leafInstance[0]
            }
            if (leafInstance == null) {
                leafInstance = [(TYPE): 'Instance']
            }
            leafInstance.instanceOf = node
            instanceLinks.relationship << [
                (TYPE): 'Relationship',
                relation: [(TYPE): 'Relation', 'label': displayText],
                object: leafInstance
            ]
        }

    }

    if (work[RELATED_TO].size() == 0) {
        work.remove(RELATED_TO)
    }
    if (addTo(work, workLinks)) save()
    if (addTo(instance, instanceLinks)) save()
}

boolean addTo(obj, links) {
    links.each { link, objects ->
        objects = getAsList(obj, link) + objects
        if (objects.size()) obj[link] = objects
    }
    return links.values().any { it }
}

List<Map> getAsList(obj, key) {
    if (obj.containsKey(key)) {
        def value = obj[key]
        return value instanceof List ? value : [value]
    } else {
        return []
    }
}
