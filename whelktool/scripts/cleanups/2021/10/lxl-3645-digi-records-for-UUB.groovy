/**
 * Retroactively create Electronic records for digital reproductions by Uka/UUB 
 * 
 * Based on the current practice
 * https://metadatabyran.kb.se/beskrivning/materialtyper-arbetsfloden/digitala-reproduktioner
 * https://github.com/libris/lxlviewer/blob/384d625e32e590984dd4b23d51763e33ff963d66/vue-client/src/utils/record.js#L131
 * 
 * See LXL-3645 for more information
 */

EOD = ['@id': 'https://libris.kb.se/library/EOD']
DIGI = ['@id': 'https://libris.kb.se/library/DIGI']
DST = ['@id': 'https://libris.kb.se/library/DST']

unhandled = getReportWriter("unhandled.txt")

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
    
    Map item = getHoldingItem(thing['@id'], 'https://libris.kb.se/library/Uka')
    if (!item) {
        unhandled.println("${bib.doc.shortId} No Uka holding")
        return
    }
    
    Set<String> uris = (asList(thing.uri) + asList(item.uri) + asList(item.hasComponent).collect{ Map i -> asList(i.uri) }.flatten()) as Set
    if (!uris) {
        unhandled.println("${bib.doc.shortId} No URI")
        return
    }
    
    // TODO: check already hasReproduction?
    
    boolean eod = record.bibliography?.remove(EOD)
    createDigitalReproduction(bib, uris, eod)
    
    boolean modified = record.bibliography?.remove(DIGI) || eod
    modified |= thing.remove('uri')
    
    if (modified) {
        bib.scheduleSave(loud:true)
    }
}

void createDigitalReproduction(bib, uris, eod) {
    def physicalThing = bib.graph[1]
    def graph = [
            [
                    '@id'                  : 'TEMP-ID',
                    '@type'                : 'Record',
                    'mainEntity'           : ["@id": "TEMP-ID#it"],
                    'bibliography'         : [DST, DIGI],
                    'encodingLevel'        : 'marc:MinimalLevel',
                    'descriptionLanguage'  : ['@id': 'https://id.kb.se/language/swe'],
                    'marc:catalogingSource': ['@id': 'https://id.kb.se/marc/CooperativeCatalogingProgram'],

            ],
            [
                    '@id'            : 'TEMP-ID#it',
                    '@type'          : 'Electronic',
                    'reproductionOf' : ['@id': physicalThing['@id']],
                    'mediaType'      : [
                            ['@id': 'https://id.kb.se/term/rda/Computer']
                    ],
                    'carrierType'    : [
                            ['@id': 'https://id.kb.se/term/rda/OnlineResource'],
                            ['@id': 'https://id.kb.se/marc/Online'],
                            ['@id': 'https://id.kb.se/marc/OnlineResource']
                    ],
                    'associatedMedia': uris.collect {
                        [
                                '@type': 'MediaObject',
                                'uri'  : [it]
                        ]
                    },
                    'production'     : [
                            '@type'   : 'DigitalReproduction',
                            'typeNote': 'Digitalt faksimil och elektronisk text',
                            'place'   : [
                                    '@type': 'Place',
                                    'label': 'Uppsala',
                            ],
                            'agent'   : [
                                    '@type': 'Agent',
                                    'label': 'Uppsala universitetsbibliotek'
                            ],
                    ]
            ],
    ]

    def digitalThing = graph[1]

    if (eod) {
        graph[0].bibliography << EOD
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

    def electronicBib = create(['@graph': graph])
    selectFromIterable([electronicBib], { d ->
        d.scheduleSave()
    })

    createHolding(electronicBib.graph[1]['@id'], 'https://libris.kb.se/library/Unix')
}

void createHolding(bibId, libraryId) {
    def graph = [
            [
                    '@id'       : 'TEMP-ID',
                    '@type'     : 'Record',
                    'mainEntity': ["@id": "TEMP-ID#it"]
            ],
            [
                    '@id'   : 'TEMP-ID#it',
                    '@type' : 'Item',
                    'itemOf': ['@id': bibId],
                    'heldBy': ['@id': libraryId],
            ],
    ]
    selectFromIterable([create(['@graph': graph])], { d -> d.scheduleSave() })
}

Map getHoldingItem(bibId, libraryId) {
    String where = """
        collection = 'hold' 
        AND deleted = false
        AND data#>>'{@graph,1,itemOf,@id}' = '$bibId'
        AND data#>>'{@graph,1,heldBy,@id}' = '$libraryId'
    """

    Map item = null
    selectBySqlWhere(where) { hold ->
        item = hold.graph[1]
    }
    
    return item
}

def asList(x) {
    (x ?: []).with {it instanceof List ? it : [it] }
}