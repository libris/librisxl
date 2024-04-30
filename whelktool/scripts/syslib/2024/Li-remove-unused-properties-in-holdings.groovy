File bibIDs = new File(scriptDir, "Li-6.txt")

selectByIds( bibIDs.readLines() ) { hold ->
def item = hold.graph[1]

final String sigel = "https://libris.kb.se/library/Li"
final List<String> propertiesToMoveToFirstComponent = ["hasNote"]

final List<String> fieldsToRemove = ["hasComponent","shelfMark", "physicalLocation", "shelfControlNumber", "shelvingControlNumber", "inventoryLevel", "marc:copyReport", "marc:completeness", "marc:lendingPolicy", "marc:shelvingScheme", "marc:retentionPolicy", "marc:acquisitionMethod", "marc:acquisitionStatus", "marc:reproductionPolicy", "marc:completeness", "classification"]
//final List<String> componentFieldsToRemove = ["shelfMark", "physicalLocation", "shelfControlNumber", "shelvingControlNumber"]

if (item["hasComponent"]) {
    List component = item["hasComponent"]

    component?.each { c ->
    if (c['hasNote']) {
      item['hasNote'] = (item['hasNote'] ?: []) + c['hasNote']
    }
    if (c['cataloguersNote']) {
      item['cataloguersNote'] = (item['cataloguersNote'] ?: []) + c['cataloguersNote']
    }
  }
}

fieldsToRemove.each {
        removeFieldFromItem(hold, it)
}

setComponent(item, sigel)
moveToComponent(item, propertiesToMoveToFirstComponent)
    
hold.scheduleSave(loud: true)
	
}

private void removeFieldFromItem(documentItem, fieldName) {
    def item = documentItem.doc.data['@graph'][1]
    item.remove(fieldName) 
}

private void removeFieldFromItemComponent(documentItem, fieldName) {
    def comp = documentItem.doc.data['@graph'][1]
    comp.remove(fieldName) 
}

void moveToComponent(Map mainEntity, List<String> propertiesToMoveToFirstComponent) {
    if (mainEntity["hasComponent"] instanceof List && mainEntity["hasComponent"].size() != 1)
        return // perhaps print warning of strange data
    Map component = mainEntity["hasComponent"][0]

    for (String propToMove : propertiesToMoveToFirstComponent) {
        if (mainEntity[propToMove]) {
            component[propToMove] = mainEntity[propToMove]
            mainEntity.remove(propToMove)
        }
    }

    if (component.isEmpty()) {
        mainEntity.remove("hasComponent")
    }
}

void setComponent(Map mainEntity, String sigel) {
    mainEntity["heldBy"] = ["@id": sigel]
    if (!mainEntity["hasComponent"])
        mainEntity["hasComponent"] = [[:]]
    if (mainEntity["hasComponent"] && mainEntity["hasComponent"] instanceof List) {
        mainEntity["hasComponent"].each { component ->
            component["heldBy"] = ["@id": sigel]
            component["@type"] = ["Item"]
        }
    }
}
