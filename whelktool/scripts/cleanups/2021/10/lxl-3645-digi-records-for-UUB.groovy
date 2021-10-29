EOD = ['@id': 'https://libris.kb.se/library/EOD']
DIGI = ['@id': 'https://libris.kb.se/library/DIGI']
DST = ['@id': 'https://libris.kb.se/library/DST']


String whereBib = """
    collection = 'bib'
    AND data#>>'{@graph,1,@type}' != 'Electronic'
    AND (data#>'{@graph,0,bibliography}' @> '[{"@id":"https://libris.kb.se/library/SPDT"}]'
        OR data#>'{@graph,0,bibliography}' @> '[{"@id":"https://libris.kb.se/library/DIGI"}]')
"""

selectBySqlWhere(whereBib) { bib ->
    def (record, thing) = bib.graph

    boolean publishedInSweden = thing.publication?.any { it.country?."@id" == "https://id.kb.se/country/sw" }
    if (!publishedInSweden) {
        return
    }
    
    Map holding = ukaHolding(thing['@id'])
    if (!holding) {
        return
    }
    
    Set<String> uris = (asList(thing.uri) + asList(holding.uri) + asList(holding.hasComponent).collect{ Map item -> asList(item.uri) }.flatten()) as Set
    if (!uris) {
        return
    }
    
    // TODO: check already hasReproduction?
    
    boolean eod = record.bibliography?.remove(EOD)
    createDigitalReproduction(bib, uris, eod)
    
    boolean modified = record.bibliography?.remove(DIGI) || eod
    modified &= thing.remove('uri')
    
    if (modified) {
        bib.scheduleSave(loud:true)
    }
}

void createDigitalReproduction(bib, uris, eod) {
    def physicalThing = bib.graph[1]
    def data = [
            [
                    '@type'       : 'Record',
                    'mainEntity'  : 'TEMP-ID',
                    'bibliography': [DST, DIGI],
                    'encodingLevel': 'marc:MinimalLevel',
                    'descriptionLanguage': [ '@id': 'https://id.kb.se/language/swe' ],
                    'marc:catalogingSource': [ '@id': 'https://id.kb.se/marc/CooperativeCatalogingProgram' ],
                    
            ],
            [
                    '@id'           : 'TEMP-ID',
                    '@type'         : 'Electronic',
                    'reproductionOf': ['@id': physicalThing['@id']],
                    'mediaType': [
                            [ '@id': 'https://id.kb.se/term/rda/Computer' ]
                    ],
                    'carrierType': [
                            [ '@id': 'https://id.kb.se/term/rda/OnlineResource' ],
                            [ '@id': 'https://id.kb.se/marc/Online' ],
                            [ '@id': 'https://id.kb.se/marc/OnlineResource' ]
                    ],
                    'associatedMedia': uris.collect {
                        [
                                '@type': 'MediaObject',
                                'uri'  : [it]
                        ]
                    },
                    'production': [
                            '@type': 'DigitalReproduction',
                            'place': [
                                    '@type': 'Place',
                                    'label': 'Uppsala',
                            ],
                            'agent': ['@id': 'https://libris.kb.se/library/Uka'],
                    ]
            ],
    ]

    def digitalThing = data[1]
    
    if (eod) {
        data[0].bibliography << EOD
    }
    
    // 😿
    [
            'instanceOf',
            'hasTitle',
            'responsibilityStatement',
            'extent'
    ].each { p ->
        if (physicalThing[p]) {
            digitalThing[p] = physicalThing[p]
        }
    }
    
    def primaryPublication = asList(physicalThing['publication']).find { it['@type'] == 'PrimaryPublication' }
    if (primaryPublication) {
        digitalThing['publication'] = primaryPublication
    }
    
    def ids = (asList(physicalThing['identifiedBy']) + asList(physicalThing['indirectlyIdentifiedBy'])) as Set
    if (ids) {
        digitalThing['indirectlyIdentifiedBy'] = ids.collect()
    }

    def electronicBib = create(data)
    selectFromIterable([electronicBib], { d ->
        d.scheduleSave()
    })
    
    def holdData = [
            [
                    '@type'     : 'Record',
                    'mainEntity': 'TEMP-ID',
            ],
            [
                    '@id'   : 'TEMP-ID',
                    '@type' : 'Item',
                    'itemOf': ['@id': electronicBib.graph[1]['@id']],
                    'heldBy': ['@id': 'https://libris.kb.se/library/Unix'],
            ],
    ]
    def hold = create(holdData)
    selectFromIterable([hold], { d ->
        d.scheduleSave()
    })
}

Map ukaHolding(bibId) {
    String where = """
        collection = 'hold' 
        AND deleted = false
        AND data#>>'{@graph,1,itemOf,@id}' = '$bibId'
        AND data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Uka'
    """

    Map item = null
    selectBySqlWhere(where) { hold ->
        item = hold.graph[1]
    }
    
    return item
}

/*

    boolean modified
    boolean hasHoldForUka
    Map params =
            [
                    'bibliographyCode'   : "DST",
                    'heldById'           : "https://libris.kb.se/library/Unix",
                    'mediaObjectUris'    : [],
                    'reproductionAgent'  : "https://libris.kb.se/library/Uka",
                    'reproductionComment': "Digitalt faksimil",
//                    'year'                  : ""
            ]
    
    

 */

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}