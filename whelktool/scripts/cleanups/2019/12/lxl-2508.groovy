/*
Using as template (after newly created Item):
{
    "@graph": [
        {
            "@id": "https://libris-dev.kb.se/gsf7dx05dvppsfp1",
            "@type": "Record",
            "created": "2019-12-10T12:22:28.912+01:00",
            "modified": "2019-12-10T12:22:28.912+01:00",
            "mainEntity": {
                "@id": "https://libris-dev.kb.se/gsf7dx05dvppsfp1#it"
            },
            "recordStatus": "marc:New",
            "controlNumber": "gsf7dx05dvppsfp1",
            "descriptionCreator": {
                "@id": "https://libris.kb.se/library/Utb1"
            },
            "descriptionLastModifier": {
                "@id": "https://libris.kb.se/library/Utb1"
            }
        },
        {
            "@id": "https://libris-dev.kb.se/gsf7dx05dvppsfp1#it",
            "@type": "Item",
            "heldBy": {
                "@id": "https://libris.kb.se/library/Utb1"
            },
            "itemOf": {
                "@id": "https://libris-dev.kb.se/s931wfw45jd6r23#it"
            },
            "hasComponent": [
                {
                    "@type": "Item",
                    "heldBy": {
                        "@id": "https://libris.kb.se/library/Utb1"
                    }
                }
            ]
        }
    ]
}
*/

PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")

def holdList = []

String where = "data#>'{@graph,0,bibliography}' @> '[{\"@type\": \"Library\", \"sigel\": \"SUEC\"}]'"

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
                                            "heldBy": ["@id": "https://libris.kb.se/library/S"]
                                    ]
                            ]
                    ]
            ]]

    holdList.add( create(holdData) )
}

selectFromIterable(holdList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave(onError: { e ->
        failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
    })
})