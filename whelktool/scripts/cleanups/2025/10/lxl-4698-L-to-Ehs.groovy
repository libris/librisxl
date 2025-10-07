PrintWriter report = getReportWriter("moveOrSplitEhs.txt")

String where = "collection = 'hold' and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/L' and data::text like '%Einar Hansens bibliotek%'"

selectBySqlWhere(where) { doc ->

    boolean moveWholeRecord = true

    // Clear the stuff that should no longer be anywhere.
    if (doc.graph[1].containsKey("usageAndAccessPolicy")) {
        doc.graph[1].remove("usageAndAccessPolicy")
        report.println("  clearing usageAndAccessPolicy on: " + doc.doc.id)
    }
    if (doc.graph[1].containsKey("hasNote")) {
        report.println("  Considering clearing hasNote " + doc.graph[1].hasNote + " on: " + doc.doc.id)
        asList(doc.graph[1].hasNote).removeAll { note ->
            return note.label.startsWith("Svenskt bokmuseum")
        }
        if (doc.graph[1].hasNote.isEmpty()) {
            doc.graph[1].remove("hasNote")
            report.println("    clear: YES ")
        }
    }

    moveWholeRecord &= ( isEhsComponent(doc.graph[1]) || !doc.graph[1].containsKey("physicalLocation") )
    if (doc.graph[1]?.hasComponent) {
        doc.graph[1]?.hasComponent.each { component ->
            moveWholeRecord &= isEhsComponent(component)
        }
    }

    if (moveWholeRecord) {
        //report.println("MOVE: " + doc.doc.id)
        moveToEhs(doc)
    } else {
        //report.println("SPLIT: " + doc.doc.id)
        splitToEhs(doc)
    }

    doc.scheduleSave(loud: true)
}

boolean isEhsComponent(Map item) {
    boolean isEhs = false
    asList(item?.physicalLocation).each {
        isEhs |= it.startsWith("Einar Hansens bibliotek")
    }
    return isEhs
}

void moveToEhs(Object doc) {
    doc.graph[1].heldBy["@id"] = "https://libris.kb.se/library/Ehs"
}

void splitToEhs(Object doc) {

    def newData =
            [ "@graph": [
                    [
                            "@id": "TEMPID",
                            "@type": "Record",
                            "mainEntity" : ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id": "TEMPID#it",
                            "@type": "Item",
                            "heldBy": ["@id": "https://libris.kb.se/library/Ehs"],
                            "itemOf": ["@id": doc.graph[1]["itemOf"]["@id"]]
                    ]
            ]]

    if (doc.graph[1].containsKey("physicalLocation")) {
        newData["@graph"][1].put( "physicalLocation", doc.graph[1].physicalLocation )
        doc.graph[1].remove("physicalLocation")
    }

    if (doc.graph[1].containsKey("hasNote")) {
        newData["@graph"][1].put( "hasNote", doc.graph[1].hasNote )
        doc.graph[1].remove("hasNote")
    }

    if (doc.graph[1].containsKey("marc:hasBindingInformation")) {

        if (doc.graph[1]["marc:hasBindingInformation"] instanceof List) {
            doc.graph[1]["marc:hasBindingInformation"].removeAll {
                if (it.containsKey("label")) {
                    if (asList(it["label"])[0].startsWith("EHB") || asList(it["label"])[0].startsWith("SVB")) {
                        if (!newData["@graph"][1].containsKey("marc:hasBindingInformation")) {
                            newData["@graph"][1].put("marc:hasBindingInformation", [])
                        }
                        newData["@graph"][1]["marc:hasBindingInformation"].add(it)
                        return true
                    }
                }
                return false
            }
        } else { // not list
            newData["@graph"][1].put( "marc:hasBindingInformation", doc.graph[1]["marc:hasBindingInformation"] )
            doc.graph[1].remove("marc:hasBindingInformation")
        }
    }

    if (doc.graph[1].containsKey("marc:hasCopyAndVersionIdentificationNote")) {
        newData["@graph"][1].put( "marc:hasCopyAndVersionIdentificationNote", doc.graph[1]["marc:hasCopyAndVersionIdentificationNote"] )
        doc.graph[1].remove("marc:hasCopyAndVersionIdentificationNote")
    }


    if (doc.graph[1]?.hasComponent) {
        doc.graph[1]?.hasComponent.removeAll { component ->
            if (component.containsKey("physicalLocation")) {

                if (!newData.containsKey("hasComponent")) {
                    newData["@graph"][1].put("hasComponent", [])
                }

                component["heldBy"]["@id"] = "https://libris.kb.se/library/Ehs"
                newData["@graph"][1]["hasComponent"].add(component)
                return true
            }
            return false
        }
    }

    def item = create(newData)
    def itemList = [item]
    selectFromIterable(itemList, { newlyCreatedItem ->
        newlyCreatedItem.scheduleSave(loud: true)

        //System.err.println("Broke: " + newData)
        //System.err.println("Out of (remains): " + doc.doc.dataAsString)

    })

}
