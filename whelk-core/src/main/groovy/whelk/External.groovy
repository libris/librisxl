package whelk

class External {
    Whelk whelk

    private static final Map IDS = [
            'https://www.wikidata.org/wiki/Q54344'   : ['@type': 'Place', 'prefLabel': 'Skellefteå', isPartOf: [['@id': 'https://www.wikidata.org/wiki/Q104877']]],
            'https://www.wikidata.org/wiki/Q1615045' : ['@type': 'Place', 'prefLabel': 'Robertsfors', isPartOf: [['@id': 'https://www.wikidata.org/wiki/Q104877']]],
            'https://www.wikidata.org/wiki/Q660050'  : ['@type': 'Place', 'prefLabel': 'Varuträsk'],
            'https://www.wikidata.org/wiki/Q10524743': ['@type': 'Place', 'prefLabel': 'Hjoggböle'],
            'https://www.wikidata.org/wiki/Q26268'   : ['@type': 'Place', 'prefLabel': 'Luleå', isPartOf: [['@id': 'https://www.wikidata.org/wiki/Q103686']]],
            'https://www.wikidata.org/wiki/Q103686'  : ['@type': 'Place', 'prefLabel': 'Norrbotten'],
            'https://www.wikidata.org/wiki/Q104877'  : ['@type': 'Place', 'prefLabel': 'Västerbotten'],
            'https://www.wikidata.org/wiki/Olov'     : ['@type': 'Place', 'prefLabel': 'Mordor', isPartOf: [['@id': 'https://www.wikidata.org/wiki/Q1036456']]],
    ]

    External(Whelk whelk) {
        this.whelk = whelk
    }

    private static Optional<Map> getThing(String iri) {
        if (iri in IDS) {
            return Optional.of(
                    IDS[iri].with {
                        it['@id'] = iri
                        new HashMap<>(it)
                    }
            )
        }
        return Optional.empty()
    }

    Optional<Document> get(String iri) {
        getThing(iri).map { document(it, JsonLd.CACHE_RECORD_TYPE) }
    }

    Optional<Document> getEphemeral(String iri) {
        getThing(iri).map {
            def d = document(it, JsonLd.CACHE_RECORD_TYPE)
            d.setRecordId("${d.getRecordIdentifiers().first()}#record".toString())
            d
        }
    }

    static Document getPlaceholder(String iri) {
        def thing = [
                '@id'  : iri,
                '@type': JsonLd.PLACEHOLDER_ENTITY_TYPE
        ]

        document(thing, JsonLd.PLACEHOLDER_RECORD_TYPE)
    }

    private static Document document(Map thing, String recordType) {
        new Document([
                '@graph': [
                        [
                                '@id'       : Document.BASE_URI.toString() + IdGenerator.generate(),
                                '@type'     : recordType,
                                'mainEntity': ['@id': thing.'@id'],
                                'inDataset' : ['@id': 'http://kblocalhost.kb.se:5000/v8h8lr6js3cmfvvd#it']
                        ],
                        thing
                ]
        ])
    }
}
