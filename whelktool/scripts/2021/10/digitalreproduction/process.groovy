/**
 * Process for creating digital reproductions
 */

import whelk.datatool.DocumentItem

String getMainEntityId(doc) { doc.graph[0].mainEntity[ID] }

Map ref(id) { [(ID): id] }

DocumentItem createFromMainEntity(entity, recordDetails=null) {
    def id = "https://id.kb.se/TEMPID"
    entity[ID] = "${id}#it"
    def record = [
        (TYPE): 'Record',
        (ID): id,
        mainEntity: [(ID): entity[ID]]
    ]
    if (recordDetails) {
        record += recordDetails
    }
    doc = create([(GRAPH): [record, entity]])
    return doc
}

DocumentItem extractWork(doc) {
    def instance = doc.graph[1]
    def workDoc = createFromMainEntity(instance.instanceOf)
    instance.instanceOf = [(ID): workDoc.graph[0].mainEntity[ID]]
    doc.scheduleSave()
    return workDoc
}

void createDigitalRepresentation(doc, params) {
    def workDoc = extractWork(doc)

    def digital = [
      (TYPE): 'Electronic',
      issuanceType: 'Monograph',
      // TODO: simplify: infer from broader of carrierType ...
      //mediaType: [ ref('https://id.kb.se/term/rda/Computer') ],
      carrierType: [ ref('https://id.kb.se/term/rda/OnlineResource') ],
      instanceOf: ref(getMainEntityId(workDoc)),
      reproductionOf: ref(getMainEntityId(doc)),
      reproduction: [
          [
            (TYPE): 'DigitalReproduction',
            label: params.reproductionComment,
            agent: [ [ (TYPE): 'Agent', label: params.reproductionAgentLabel ] ],
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
          //'marc:publicNote': 'Fritt tillgänglig via Kungliga biblioteket',
          uri: [params.mediaObjectUri]
        ]
      ]
    }
    if (params.thumbnailUrl) {
      //digital.isPrimaryTopicOf = [
      //  [ (TYPE): "Document","cataloguersNote": ["digipic"],"marc:publicNote": [params.tumnagelNote],uri: [params.thumbnailUrl] ]
      //]
      digital.thumbnail = [
        [ (TYPE): "MediaObject", comment: [params.tumnagelNote], uri: [params.thumbnailUrl] ]
      ]
    }

    // TODO: define a reasonable set of licenses/policies/rights
    digital.usageAndAccessPolicy = ref('https://id.kb.se/license/freely-available')

    def recordDetails = [
      bibliography: [
        [(ID): 'https://libris.kb.se/library/DIGI']
      ],
      //hasNote: [
      //  //[(TYPE): 'Note', label: 'Digitaliserat exemplar'],
      //  [(TYPE): 'Note', label: 'Fritt tillgänglig via Internet']
      //]
    ]

    if (params.bibliographyCode) {
      recordDetails.bibliography << [
        [(ID): 'https://libris.kb.se/library/${params.bibliographyCode}']
      ]
    }

    def digiDoc = createFromMainEntity(digital, recordDetails)

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
        createFromMainEntity(item, holdRecordDetails)
    }
}

process &createDigitalRepresentation
