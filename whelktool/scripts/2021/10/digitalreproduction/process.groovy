/**
 * Process for creating digital reproductions
 */

import whelk.datatool.DocumentItem

String getMainEntityId(doc) { doc.graph[0].mainEntity[ID] }

Map ref(id) { [(ID): id] }

DocumentItem createFromMainEntity(script, entity, recordDetails=null) {
    def id = "TEMPID"
    entity[ID] = "${id}#it"

    def record = [
        (ID): id,
        (TYPE): 'Record',
        mainEntity: [(ID): entity[ID]]
    ]
    if (recordDetails) {
        record += recordDetails
    }

    return script.create([(GRAPH): [record, entity]])
}

DocumentItem extractWork(script, doc) {
    def record = doc.graph[0]
    def instance = doc.graph[1]
    def work = instance.instanceOf

    if (!work.containsKey('hasTitle')) {
        def title = instance.hasTitle?.find { it[TYPE] == 'Title' }
        if (title) {
            title = title.clone()
            title.source = [ ref(instance[ID]) ]
            work.hasTitle = [ title ]
        }
    }

    def recordDetails = [
      derivedFrom: ref(record[ID])
    ]
    return createFromMainEntity(script, work, recordDetails)
}

void createDigitalRepresentation(Script script, DocumentItem originalDoc, Map params) {
    def newlyCreated = []

    def original = originalDoc.graph[1]

    if (!original.instanceOf.containsKey(ID)) {
        def workDoc = extractWork(script, originalDoc)
        original.instanceOf = ref(getMainEntityId(workDoc))
        originalDoc.scheduleSave()
        newlyCreated << workDoc
    }

    def digital = [
      (TYPE): 'Electronic',
      issuanceType: 'Monograph',
      carrierType: [ ref('https://id.kb.se/term/rda/OnlineResource') ],
      genreForm: params.genreForm,
      instanceOf: ref(original.instanceOf[ID]),
      reproductionOf: ref(getMainEntityId(originalDoc)),
      production: [
          [
            (TYPE): 'Reproduction',
            agent: [ params.reproductionAgent ],
            year: params.year,
            place: [ [ (TYPE): 'Place', label: 'Stockholm' ] ],
            country: ref('https://id.kb.se/country/sw')
          ]
      ],
    ]

    if (original.hasTitle) {
        digital.hasTitle = original.hasTitle
    }

    if (params.mediaObjectUri) {
      digital.associatedMedia = [
        [
          (TYPE): 'MediaObject',
          usageAndAccessPolicy: ref('https://id.kb.se/policy/freely-available'),
          publisher: params.mediaObjectAgent ?: params.reproductionAgent,
          uri: [params.mediaObjectUri]
        ]
      ]
    }
    if (params.imageUrl) {
      digital.isPrimaryTopicOf = [
        [ (TYPE): "Document", cataloguersNote: ["digipic"], "marc:publicNote": [params.imageNote],uri: [params.imageUrl] ]
      ]
    }

    def recordDetails = [
      bibliography: [
        [(ID): 'https://libris.kb.se/library/DIGI']
      ],
    ]

    if (params.bibliographyCode) {
      recordDetails.bibliography << [
        [(ID): "https://libris.kb.se/library/${params.bibliographyCode}" as String]
      ]
    }

    def digiDoc = createFromMainEntity(script, digital, recordDetails)
    newlyCreated << digiDoc

    if (params.heldById) {
        def item = [
        (TYPE): 'Item',
        itemOf: ref(getMainEntityId(digiDoc)),
        heldBy: ref(params.heldById)
        ]
        def holdRecordDetails = [:]
        if (params.holdingNote) {
            holdRecordDetails.cataloguersNote = [ params.holdingNote ]
        }
        def itemDoc = createFromMainEntity(script, item, holdRecordDetails)
        newlyCreated << itemDoc
    }

    script.selectFromIterable(newlyCreated) { doc ->
        doc.scheduleSave()
    }
}

process this.&createDigitalRepresentation
