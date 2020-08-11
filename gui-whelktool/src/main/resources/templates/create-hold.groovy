PrintWriter failedHoldIDs = getReportWriter("failed-to-create-holdIDs")
PrintWriter createdHoldIDs = getReportWriter("attempted-to-create-holdIDs")
File bibids = new File('INPUT')

String bibidstring = bibids.readLines().join("','")

def holdList = []

selectBySqlWhere("data#>>'{@graph,0,controlNumber}' in ( '$bibidstring' )", silent: false, { bib ->

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
                            "heldBy": ["@id": "https://libris.kb.se/library/SIGEL"],
                            "itemOf": ["@id": bibMainEntity],
                            "hasComponent": [
                                    [
                                            "@type": "Item",
                                            "heldBy": ["@id": "https://libris.kb.se/library/SIGEL"]
                                    ]
                            ]
                    ]
            ]]

    holdList.add( create(holdData) )

})

selectFromIterable(holdList, { newlyCreatedItem ->
    createdHoldIDs.println(newlyCreatedItem.doc.shortId)
    newlyCreatedItem.scheduleSave(onError: { e ->
        failedHoldIDs.println("Failed to save ${newlyCreatedItem.doc.shortId} due to: $e")
    })
})