import whelk.Document

['isPartOf', 'supplementTo'].each { property ->
    String where

    if (property == 'isPartOf')
        where = "collection = 'bib' and data#>>'{@graph,1,isPartOf}' LIKE '%{}%'"
    if (property == 'supplementTo')
        where = "collection = 'bib' and data#>'{@graph,1,supplementTo}' @> '[null]'"

    selectBySqlWhere(where) { data ->
        List propertyValueOfCurrentVersion = data.graph[1][property]
        List propertyValueBeforeLoss
        List propertyValueRightAfterLoss

        // Get all versions sorted by generationDate, most recent first
        List<Document> versions = data.whelk.storage.loadAllVersions(data.doc.shortId).sort { a, b ->
            b.getGenerationDate() <=> a.getGenerationDate()
        }

        for (i in 0..<versions.size()) {
            propertyValue = versions[i].data['@graph'][1][property] ?: versions[i].data['@graph'][2][property]

            // Skip until we reach version not containing empty/null object
            if (propertyValue.any { !it })
                continue

            propertyValueBeforeLoss = propertyValue
            propertyValueRightAfterLoss = versions[i - 1].data['@graph'][1][property] ?: versions[i - 1].data['@graph'][2][property]

            break
        }

        // The property value has not changed since right after loss
        assert propertyValueOfCurrentVersion == propertyValueRightAfterLoss

        List recoveredPropertyValue = recover(propertyValueBeforeLoss)

        List linkedPropertyValue = link(recoveredPropertyValue)

        data.graph[1][property] = linkedPropertyValue

        data.scheduleSave()
    }
}

List recover(List propertyValue) {
    // Some objects don't have an instance. These are all redundant and we can safely remove them.
    propertyValue.removeAll { !it.containsKey('hasInstance') }

    List recoveredPropertyValue = propertyValue.collect { work ->
        // Obsolete property, we don't want to recover this
        work.remove('marc:toDisplayNote')

        // Invert instance-work relation
        Map instance = asList(work['hasInstance'])[0]
        work.remove('hasInstance')
        work['@type'] = 'Work' // In case type was 'Aggregate'
        Map inverse = instance << ['instanceOf': work]

        return inverse
    }

    return recoveredPropertyValue
}

List link(List propertyValue) {
    linkedPropertyValue = propertyValue.collect { part ->
        asList(part["describedBy"]).each { describedBy ->
            if (describedBy instanceof Map &&
                    describedBy["controlNumber"] &&
                    describedBy["controlNumber"] instanceof String &&
                    !describedBy["@id"]) {
                String controlNumber = sanitize(describedBy["controlNumber"])
                String properUri = findMainEntityId(controlNumber)

                if (properUri != null)
                    part = ['@id': properUri]
            }
        }
        return part
    }
    return linkedPropertyValue
}


String findMainEntityId(String ctrlNumber) {
    String mainId = null
    try {
        mainId = findCanonicalId("${baseUri.resolve(ctrlNumber)}#it")
    } catch (IllegalArgumentException e) {
    }
    if (mainId) {
        return mainId
    }
    def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    mainId = findCanonicalId(legacyId)
    if (mainId) {
        return mainId
    }

    def byLibris3Ids = []
    // IMPORTANT: This REQUIRES an index on '@graph[0]identifiedBy*.value'.
    // If that is removed, this slows to a GLACIAL crawl!
    ctrlNumber = ctrlNumber.replaceAll(/['"\\]/, '')
    selectBySqlWhere("""
    data #> '{@graph,0,identifiedBy}' @> '[{"@type": "LibrisIIINumber", "value": "${ctrlNumber}"}]'::jsonb and collection = 'bib'
    """, silent: true) {
        List mainEntityIDs = it.doc.getThingIdentifiers()
        if (mainEntityIDs.size() > 0)
            byLibris3Ids << mainEntityIDs[0]
    }
    if (byLibris3Ids.size() == 1) {
        return byLibris3Ids[0]
    }
    return null
}

String sanitize(String value) {
    return value.replaceAll(/\9/, '')
}

List asList(o) {
    return (o instanceof List) ? o : [o]
}