import whelk.datatool.DocumentItem

onlyCandidateHasLanguage = getReportWriter('only-candidate-has-language.tsv')
multipleCandidates = getReportWriter('multiple-candidates.tsv')
newHubs = getReportWriter('new-hubs.tsv')
upgradedHubs = getReportWriter('upgraded-hubs.tsv')
replacedUniformWorkTitles = getReportWriter('replaced-uniform-work-titles.tsv')

List<String> hublessTitles = []
Map<String, String> okHubToPrefTitle = [:]
Map<String, String> oldHubToPrefTitle = [:]

tsvToMap('hubs.tsv').each { prefTitle, List<Map> uniformWorkTitles ->
    // 3: No hub candidate, create new hub from given preferred title
    if (uniformWorkTitles.isEmpty()) {
        hublessTitles.add(prefTitle)
        return
    }

    // 2: Exactly one hub candidate
    if (uniformWorkTitles.size() == 1) {
        if (uniformWorkTitles[0].language) {
            // b: the candidate has language, report
            onlyCandidateHasLanguage.println([uniformWorkTitles[0].iri, uniformWorkTitles[0].language, prefTitle].join('\t'))
        } else {
            // a: no language, ok to use as hub
            okHubToPrefTitle[uniformWorkTitles[0].iri] = prefTitle
        }
        return
    }

    // 1: More than one hub candidate
    def okHubRecords = uniformWorkTitles.findResults { it.language ? null : it.iri }
    if (okHubRecords.isEmpty()) {
        // b: all candidates have language, create new hub without language and link the rejected candidates to the new hub
        hublessTitles.add(prefTitle)
        uniformWorkTitles.each {
            oldHubToPrefTitle[it.iri] = prefTitle
        }
    } else if (okHubRecords.size() == 1) {
        // a: exactly one candidate without language, ok to use as hub, link rejected candidates to this
        okHubToPrefTitle[okHubRecords[0]] = prefTitle
        uniformWorkTitles.each {
            if (it.iri != okHubRecords[0]) {
                oldHubToPrefTitle[it.iri] = prefTitle
            }
        }
    } else {
        // c: more than one candidate without language, report
        multipleCandidates.println([okHubRecords, prefTitle].join('\t'))
    }
}

Map<String, DocumentItem> prefTitleToNewHub = hublessTitles.collectEntries { [it, newHub(it)] }
Map<String, String> prefTitleToOkHub = okHubToPrefTitle.collectEntries { k, v -> [v, k] }

// Save new hubs
selectFromIterable(prefTitleToNewHub.values()) {
    def thing = it.graph[1]
    newHubs.println([thing.'@id', thing.preferredTitle].join('\t'))
    it.scheduleSave()
}

// Upgrade uniform work titles to hubs
selectByIds(okHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    //TODO: Add WorkHub and preferredTitle to vocab if they are to be used
    thing.'@type' = 'WorkHub'
    thing.preferredTitle = okHubToPrefTitle[thing.'@id']
    thing.remove('inCollection')
    upgradedHubs.println([thing.'@id', thing.remove('hasTitle'), thing.preferredTitle].join('\t'))
    it.scheduleSave()
}

// Link uniform work titles to the hubs that replace them
selectByIds(oldHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    def thingId = thing.'@id'
    def prefTitle = oldHubToPrefTitle[thingId]
    def hubId = prefTitleToNewHub[prefTitle] ?: prefTitleToOkHub[prefTitle]
    thing.expressionOf = ['@id': hubId]
    def expressions = getExpressions(thingId)
    if (expressions) {
        thing.hasExpressionInstance = expressions //TODO: Add hasExpressionInstance to vocab if it is to be used
    }
    replacedUniformWorkTitles.println([thingId, thing.hasTitle, thing.language, hubId, prefTitle].join('\t'))
    it.scheduleSave()
}

def getExpressions(String hubId) {
    return queryDocs(['instanceOf.expressionOf.@id': [hubId]]).collect { it.subMap('@id') }
}

def newHub(String prefTitle) {
    //TODO: Add relevant metadata. Title object or plain string?
    def hubData =
            ["@graph": [
                    [
                            "@id"       : "TEMPID",
                            "@type"     : "Record",
                            "mainEntity": ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id"           : "TEMPID#it",
                            "@type"         : "WorkHub",
                            "preferredTitle": prefTitle
                    ]
            ]]

    return create(hubData)
}

def tsvToMap(String filename) {
    Map hubs = [:]

    new File(scriptDir, filename).splitEachLine('\t') { row ->
        if (row.size() == 3) {
            def (localOrUniform, language, prefTitle) = row
            Map uniformWorkTitles = [:]
            if (isIri(localOrUniform)) {
                uniformWorkTitles.iri = localOrUniform.replace("https://libris.kb.se/", baseUri.toString()) // replace only needed in test environments
            }
            if (language) {
                uniformWorkTitles.language = language
            }
            def entry = hubs.computeIfAbsent(prefTitle, { k -> [] })
            if (uniformWorkTitles) {
                entry.add(uniformWorkTitles)
            }
        }
    }

    return hubs
}

def isIri(String s) {
    s.startsWith('http')
}
