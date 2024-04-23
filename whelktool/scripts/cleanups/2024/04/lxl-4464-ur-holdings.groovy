/*
 * Add holdings for all records in bibliography UR for DalE and Hde
 *
 * See LXL-4464 for more info.
 *
 */


import whelk.component.PostgreSQLComponent.ConflictingHoldException

failedHoldIDs = getReportWriter("failed-holdIDs")
noUrId = getReportWriter("no-UR-id")

ASSOCIATED_MEDIA = [
        'DalE': [
                '@type': 'MediaObject',
                'uri': ['https://access.ur.se/bibliotek/play/'],
                'marc:publicNote': ['Tillgänglig via Utbildningsradion (UR)']
        ],
        'Hde': [
                '@type': 'MediaObject',
                'uri': ['https://uraccess.net/products/'],
                'marc:publicNote': ['Online access för Högskolan Dalarna / Online access for Högskolan Dalarna']
        ]
]


String where = """
  collection = 'bib' 
  AND deleted = false
  AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/UR"}]'
"""

selectBySqlWhere(where) { bib ->
    var bibId = bib.graph[1]["@id"]

    String urId = getAtPath(bib.graph, [0, 'identifiedBy', '*', 'value']).find{ String id -> id.startsWith('UR') }
    if (!urId) {
        noUrId.println(bibId)
        return
    }
    urId = urId.substring(2)

    ASSOCIATED_MEDIA.keySet().each { createHold(bibId, it, urId) }
}

void createHold(String bibId, String sigel, String urId) {
    def holdData =
            ["@graph": [
                    [
                            "@id"             : "TEMPID",
                            "@type"           : "Record",
                            "mainEntity"      : ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id"             : "TEMPID#it",
                            "@type"           : "Item",
                            "heldBy"          : ["@id": "https://libris.kb.se/library/${sigel}".toString()],
                            "inventoryLevel"  : 1,
                            "itemOf"          : ["@id": bibId],
                            'associatedMedia' : buildAssociatedMedia(sigel, urId)
                    ]
            ]]

    selectFromIterable([create(holdData)], { newlyCreatedItem ->
        newlyCreatedItem.scheduleSave(onError: { e ->
            if (e instanceof ConflictingHoldException) {
                // ignore
            }
            else {
                failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
            }
        })
    })
}

Map buildAssociatedMedia(String sigel, String urId) {
    var a = new HashMap(ASSOCIATED_MEDIA[sigel])
    a.uri = a.uri.collect { it + urId }
    return a
}