/**
 * Process for creating digital reproductions
 */

import whelk.datatool.DocumentItem

String getMainEntityId(doc) { doc.graph[0].mainEntity[ID] }

Map ref(id) { [(ID): id] }

DocumentItem createFromMainEntity(main, entity, recordDetails=null) {
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
    doc = main.create([(GRAPH): [record, entity]])
    return doc
}

DocumentItem extractWork(main, doc) {
    def instance = doc.graph[1]
    def workDoc = createFromMainEntity(main, instance.instanceOf)
    instance.instanceOf = [(ID): workDoc.graph[0].mainEntity[ID]]
    doc.scheduleSave()
    return workDoc
}

void createDigitalRepresentation(main, printDoc, params) {
    def newlyCreated = []

    def workDoc = extractWork(main, printDoc)
    newlyCreated << workDoc

    def digital = [
      (TYPE): 'Electronic',
      issuanceType: 'Monograph',
      carrierType: [ ref('https://id.kb.se/term/rda/OnlineResource') ],
      genreForm: params.genreForm,
      instanceOf: ref(getMainEntityId(workDoc)),
      reproductionOf: ref(getMainEntityId(printDoc)),
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

    def digiDoc = createFromMainEntity(main, digital, recordDetails)
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
        def itemDoc = createFromMainEntity(main, item, holdRecordDetails)
        newlyCreated << itemDoc
    }

    main.selectFromIterable(newlyCreated) { doc ->
        doc.scheduleSave()
    }
}

process this.&createDigitalRepresentation
