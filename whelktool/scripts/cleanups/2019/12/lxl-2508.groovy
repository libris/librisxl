/*
Using as template:
{
    "@graph": [
        {
            "@type": "Record",
            "@id": "https://id.kb.se/TEMPID",
            "descriptionCreator": {
                "@id": "https://libris.kb.se/library/S"
            },
            "mainEntity": {
                "@id": "https://id.kb.se/TEMPID#it"
            }
        },
        {
            "@id": "https://id.kb.se/TEMPID#it",
            "@type": "Item",
            "heldBy": {
                "@id": "https://libris.kb.se/library/S"
            },
            "itemOf": {
                "@id": "https://libris.kb.se/0jbnrcsb04grvf4#it"
            },
            "hasComponent": [
                {
                    "@type": "Item",
                    "heldBy": {
                        "@id": "https://libris.kb.se/library/S"
                    },
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": "F\u00f6rv\u00e4rvas ej av KB (annan utg\u00e5va finns)"
                        }
                    ]
                }
            ]
        }
    ]
}
*/

PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter failedBibIDs = getReportWriter("failed-bibIDs")

def holdList = []

String where = "data#>'{@graph,0,bibliography}' @> '[{\"@type\": \"Library\", \"sigel\": \"SUEC\"}]' and data#>'{@graph,0,technicalNote}' @> '[{\"@type\": \"TechnicalNote\", \"label\": [\"S: F\\u00f6rv\\u00e4rvas ej av KB (annan utg\\u00e5va finns)\"]}]'"

selectBySqlWhere(where, silent: false) { bib ->

    def bibMainEntity = bib.graph[1]["@id"]

    def holdData =
            [ "@graph": [
                    [
                            "@id": "TEMPID",
                            "@type": "Record",
                            "mainEntity" : ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id": "TEMPID#it",
                            "@type": "Item",
                            "heldBy": ["@id": "https://libris.kb.se/library/S"],
                            "itemOf": ["@id": bibMainEntity],
                            "hasComponent": [
                                    [
                                            "@type": "Item",
                                            "heldBy": ["@id": "https://libris.kb.se/library/S"],
                                            "hasNote": [
                                                    [
                                                            "@type": "Note",
                                                            "label": "Förvärvas ej av KB (annan utgåva finns)"
                                                    ]
                                            ]
                                    ]
                            ]
                    ]
            ]]

    holdList.add( create(holdData) )

    def technicalNote = bib.graph[0]["technicalNote"]
    def it = technicalNote.iterator()
    while (it.hasNext()) {
        def element = it.next()
        if (element instanceof String && element == "S: F\\u00f6rv\\u00e4rvas ej av KB (annan utg\\u00e5va finns)")
            it.remove()
    }
    if (technicalNote.size() == 0)
        bib.graph[0].remove("technicalNote")

    bib.scheduleSave(onError: { e ->
        failedBibIDs.println("Failed to save ${bib.doc.shortId} due to: $e")
    })
}

selectFromIterable(holdList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave(onError: { e ->
        // A number of these are expected to fail, since many of the SUEC bib records already have S holdings. This is ok.
        failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
    })
})