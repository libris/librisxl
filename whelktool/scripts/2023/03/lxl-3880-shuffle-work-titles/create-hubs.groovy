import whelk.datatool.DocumentItem
import static whelk.JsonLd.looksLikeIri

onlyCandidateHasLanguage = getReportWriter('only-candidate-has-language.tsv')
multipleCandidates = getReportWriter('multiple-candidates.tsv')
newHubs = getReportWriter('new-hubs.tsv')
upgradedToHub = getReportWriter('upgraded-to-hub.tsv')
replacedUniformWorkTitles = getReportWriter('replaced-uniform-work-title.tsv')

HUB = 'WorkHub'
ID = '@id'
TYPE = '@type'
HAS_TITLE = 'hasTitle'
MAIN_TITLE = 'mainTitle'

List<String> hublessTitles = []
Map<String, String> okHubToPrefTitle = [:]
Map<String, String> oldHubToPrefTitle = [:]

tsvToMap('hub-data/hubs.tsv').each { hubTitle, data ->
    def uniformWorkTitles = data.uniformWorkTitles

    // No hub candidate, create new hub from given preferred title
    if (uniformWorkTitles.isEmpty()) {
        hublessTitles.add(hubTitle)
        return
    }

    // Exactly one hub candidate
    if (uniformWorkTitles.size() == 1) {
        if (uniformWorkTitles[0].language) {
            // The candidate has language, report
            onlyCandidateHasLanguage.println([hubTitle, uniformWorkTitles[0].iri, uniformWorkTitles[0].language].join('\t'))
        } else {
            // No language, ok to use as hub
            okHubToPrefTitle[uniformWorkTitles[0].iri] = hubTitle
        }
        return
    }

    // More than one hub candidate
    def okHubRecords = uniformWorkTitles.findResults { it.language ? null : it.iri }
    if (okHubRecords.isEmpty()) {
        // All candidates have language, create new hub without language and link the rejected candidates to the new hub
        hublessTitles.add(hubTitle)
        uniformWorkTitles.each {
            oldHubToPrefTitle[it.iri] = hubTitle
        }
    } else if (okHubRecords.size() == 1) {
        // Exactly one candidate without language, ok to use as hub, link rejected candidates to this
        okHubToPrefTitle[okHubRecords[0]] = hubTitle
        uniformWorkTitles.each {
            if (it.iri != okHubRecords[0]) {
                oldHubToPrefTitle[it.iri] = hubTitle
            }
        }
    } else {
        // More than one candidate without language, report
        multipleCandidates.println([hubTitle, okHubRecords].join('\t'))
    }
}

Map<String, DocumentItem> hubTitleToNewHub = hublessTitles.collectEntries { [it, newHub(it)] }
Map<String, String> hubTitleToOkHub = okHubToPrefTitle.collectEntries { k, v -> [v, k] }

// Save new hubs
selectFromIterable(hubTitleToNewHub.values()) {
    def thing = it.graph[1]
    newHubs.println([thing[ID], thing[HAS_TITLE]].join('\t'))
    it.scheduleSave()
}

// Upgrade uniform work titles to hubs
selectByIds(okHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    def thingId = thing[ID]
    upgradedToHub.println([thingId, thing[HAS_TITLE], okHubToPrefTitle[thingId]].join('\t'))
    upgradeHub(thing, okHubToPrefTitle[thingId])
    it.scheduleSave()
}

// Link uniform work titles to the hubs that replace them
selectByIds(oldHubToPrefTitle.keySet()) {
    def thing = it.graph[1]
    def thingId = thing[ID]
    def hubTitle = oldHubToPrefTitle[thingId]
    def hubId = hubTitleToOkHub[hubTitle] ?: hubTitleToNewHub[hubTitle].graph[1][ID]
    thing.expressionOf = [(ID): hubId]
    //TODO: Link back to expressions somehow
//    def expressions = getExpressions(thingId)
//    if (expressions) {
//        thing.hasExpressionInstance = expressions
//    }
    replacedUniformWorkTitles.println([thingId, thing[HAS_TITLE], thing.language, hubId, hubTitle].join('\t'))
    it.scheduleSave()
}

def getExpressions(String hubId) {
    return queryDocs(['instanceOf.expressionOf.@id': [hubId]]).collect { it.subMap(ID) }
}

def upgradeHub(Map hub, String hubTitle) {
    hub[TYPE] = HUB
    hub[HAS_TITLE] = [[(TYPE): 'Title', (MAIN_TITLE): hubTitle]]
    hub.remove('inCollection')
    //TODO: Remove/move/add more metadata
}

def newHub(String hubTitle) {
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
                                            (MAIN_TITLE): hubTitle
                                    ]
                            ]
                    ]
            ]]

    return create(hubData)
}

def tsvToMap(String filename) {
    Map hubs = [:]
    new File(scriptDir, filename).readLines().drop(1).each { row ->
        def (localOrUniform, language, hubTitle, source) = row.split('\t', -1)
        Map uniformWorkTitle = [:]
        if (looksLikeIri(localOrUniform)) {
            // replace only needed in test environments
            uniformWorkTitle.iri = localOrUniform.replace("https://libris.kb.se/", baseUri.toString())
            if (language) {
                uniformWorkTitle.language = language
            }
        }
        def entry = hubs.computeIfAbsent(hubTitle, { k -> ['uniformWorkTitles': []] })
        if (uniformWorkTitle) {
            entry.uniformWorkTitles.add(uniformWorkTitle)
        }
        if (source) {
            entry.source = source
        }
    }

    return hubs
}
