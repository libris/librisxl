import whelk.datatool.DocumentItem
import static whelk.JsonLd.looksLikeIri

onlyCandidateHasLanguage = getReportWriter('only-candidate-has-language.tsv')
multipleCandidates = getReportWriter('multiple-candidates.tsv')
newHubs = getReportWriter('new-hubs.tsv')
upgradedHubs = getReportWriter('upgraded-hubs.tsv')
replacedUniformWorkTitles = getReportWriter('replaced-uniform-work-titles.tsv')

HUB = 'Hub' //TODO: Change to 'WorkHub' when added to vocab
ID = '@id'
TYPE = '@type'
HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'

List<String> hublessTitles = []
Map<String, String> okHubToPrefTitle = [:]
Map<String, String> oldHubToPrefTitle = [:]

tsvToMap('hubs.tsv').each { prefTitle, data ->
    def uniformWorkTitles = data.uniformWorkTitles

    // No hub candidate, create new hub from given preferred title
    if (uniformWorkTitles.isEmpty()) {
        hublessTitles.add(prefTitle)
        return
    }

    // Exactly one hub candidate
    if (uniformWorkTitles.size() == 1) {
        if (uniformWorkTitles[0].language) {
            // The candidate has language, report
            onlyCandidateHasLanguage.println([prefTitle, uniformWorkTitles[0].iri, uniformWorkTitles[0].language].join('\t'))
        } else {
            // No language, ok to use as hub
            okHubToPrefTitle[uniformWorkTitles[0].iri] = prefTitle
        }
        return
    }

    // More than one hub candidate
    def okHubRecords = uniformWorkTitles.findResults { it.language ? null : it.iri }
    if (okHubRecords.isEmpty()) {
        // All candidates have language, create new hub without language and link the rejected candidates to the new hub
        hublessTitles.add(prefTitle)
        uniformWorkTitles.each {
            oldHubToPrefTitle[it.iri] = prefTitle
        }
    } else if (okHubRecords.size() == 1) {
        // Exactly one candidate without language, ok to use as hub, link rejected candidates to this
        okHubToPrefTitle[okHubRecords[0]] = prefTitle
        uniformWorkTitles.each {
            if (it.iri != okHubRecords[0]) {
                oldHubToPrefTitle[it.iri] = prefTitle
            }
        }
    } else {
        // More than one candidate without language, report
        multipleCandidates.println([prefTitle, okHubRecords].join('\t'))
    }
}

Map<String, DocumentItem> prefTitleToNewHub = hublessTitles.collectEntries { [it, newHub(it)] }
Map<String, String> prefTitleToOkHub = okHubToPrefTitle.collectEntries { k, v -> [v, k] }

// Save new hubs
selectFromIterable(prefTitleToNewHub.values()) {
    def thing = it.graph[1]
    newHubs.println([thing[ID], thing[HAS_TITLE][0][MAIN_TITLE]].join('\t'))
    it.scheduleSave()
}

// Upgrade uniform work titles to hubs
selectByIds(okHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    def thingId = thing[ID]
    upgradedHubs.println([thingId, thing[HAS_TITLE], okHubToPrefTitle[thingId]].join('\t'))
    upgradeHub(thing, okHubToPrefTitle[thingId])
    it.scheduleSave()
}

// Link uniform work titles to the hubs that replace them
selectByIds(oldHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    def thingId = thing[ID]
    def prefTitle = oldHubToPrefTitle[thingId]
    def hubId = prefTitleToOkHub[prefTitle] ?: prefTitleToNewHub[prefTitle].graph[1][ID]
    thing.expressionOf = [(ID): hubId]
    //TODO: Link back to expressions somehow
//    def expressions = getExpressions(thingId)
//    if (expressions) {
//        thing.hasExpressionInstance = expressions
//    }
    replacedUniformWorkTitles.println([thingId, thing[HAS_TITLE], thing.language, hubId, prefTitle].join('\t'))
    it.scheduleSave()
}

def getExpressions(String hubId) {
    return queryDocs(['instanceOf.expressionOf.@id': [hubId]]).collect { it.subMap(ID) }
}

def upgradeHub(Map hub, String prefTitle) {
    hub[TYPE] = HUB
    hub[HAS_TITLE] = [[(TYPE): 'Title', (MAIN_TITLE): prefTitle]]
    hub.remove('inCollection')
}

def newHub(String prefTitle) {
    //TODO: Add other relevant metadata. Provenience?
    def hubData =
            ["@graph": [
                    [
                            (ID)        : "TEMPID",
                            (TYPE)      : "Record",
                            "mainEntity": ["@id": "TEMPID#it"]
                    ],
                    [
                            (ID)       : "TEMPID#it",
                            (TYPE)     : HUB,
                            (HAS_TITLE): [
                                    [
                                            (TYPE)      : 'Title',
                                            (MAIN_TITLE): prefTitle
                                    ]
                            ]
                    ]
            ]]

    return create(hubData)
}

def tsvToMap(String filename) {
    Map hubs = [:]

    new File(scriptDir, filename).splitEachLine('\t') { row ->
        if (row.size() > 3) {
            def (localOrUniform, language, source, prefTitle) = row.take(4)
            Map uniformWorkTitles = [:]
            if (looksLikeIri(localOrUniform)) {
                // replace only needed in test environments
                uniformWorkTitles.iri = localOrUniform.replace("https://libris.kb.se/", baseUri.toString())
            }
            if (language) {
                uniformWorkTitles.language = language
            }
            def entry = hubs.computeIfAbsent(prefTitle.trim(), { k -> ['uniformWorkTitles': []] })
            if (source) {
                entry.source = source
            }
            if (uniformWorkTitles) {
                entry.uniformWorkTitles.add(uniformWorkTitles)
            }
        }
    }

    return hubs
}
