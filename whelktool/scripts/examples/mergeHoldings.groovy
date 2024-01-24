import whelk.Document

/********* Settings section, you will need to provide values for these variables **********/

// Define your list of instances for which to merge holdings, you probably want one of
// these forms:
final List<String> bibIds = ["bbbbbbbbb", "cccccccc"]
//List<String> bibIds = new File('/some/path or filename in same directory').readLines()

// Define which libraries/sigel are concerned.
final String libraryToMoveFrom = "https://libris.kb.se/library/SOMESIGEL"
final String libraryToMoveTo = "https://libris.kb.se/library/OTHERSIGEL"

// Should resulting changes be exported ?
final boolean isLoud = false

// Move some stuff from mainEntity to mainEntity.hasComponent[0] before doing the merging/replacing ?
final List<String> propertiesToMoveToFirstComponent = ["shelfMark"]

// Which policy should apply to which properties (under mainEntity and hasComponent[0]) ?
// Properties that are not listed (default) will keep their current values (of the record we're trying to merge into).
final List<String> propertiesToMerge = []
final List<String> propertiesToReplace = ["shelfMark"]





/********* Code section (you should not need to change anything below this point) **********/
for (String bibId : bibIds) {
    selectBySqlWhere(" collection = 'hold' and data#>>'{@graph,1,heldBy,@id}' = '${libraryToMoveFrom}' and data#>>'{@graph,1,itemOf,@id}' = '${Document.getBASE_URI().resolve(bibId)}#it'",
            silent: false, { incomingHold ->
        selectBySqlWhere(" collection = 'hold' and data#>>'{@graph,1,heldBy,@id}' = '${libraryToMoveTo}' and data#>>'{@graph,1,itemOf,@id}' = '${Document.getBASE_URI().resolve(bibId)}#it'",
                silent: false, { targetHold ->

            //Document targetHoldBefore = targetHold.doc.clone()
            combine(targetHold.doc, incomingHold.doc, propertiesToMerge, propertiesToReplace, propertiesToMoveToFirstComponent)
            //System.err.println("Before merge:\n\t" + targetHoldBefore.getDataAsString() + "\nAfter merge:\n\t" + targetHold.doc.getDataAsString() + "\n")
            targetHold.scheduleSave(loud: isLoud)
        })
        incomingHold.scheduleDelete(loud: isLoud)
    })
}

void combine(Document target, Document incoming, List<String> propertiesToMerge, List<String> propertiesToReplace, List<String> propertiesToMoveToFirstComponent) {
    Map incomingMainEntity = incoming.data["@graph"][1]
    Map targetMainEntity = target.data["@graph"][1]

    moveToComponent(incomingMainEntity, propertiesToMoveToFirstComponent)
    moveToComponent(targetMainEntity, propertiesToMoveToFirstComponent)

    combineMap(incomingMainEntity, targetMainEntity, propertiesToMerge, propertiesToReplace)
    if (incomingMainEntity["hasComponent"] && incomingMainEntity["hasComponent"] instanceof List && incomingMainEntity["hasComponent"][0] &&
            targetMainEntity["hasComponent"] && targetMainEntity["hasComponent"] instanceof List && targetMainEntity["hasComponent"][0])
        combineMap(targetMainEntity["hasComponent"][0], incomingMainEntity["hasComponent"][0], propertiesToMerge, propertiesToReplace)
}

void combineMap(Map target, Map incoming, List<String> propertiesToMerge, List<String> propertiesToReplace) {
    for (String propToReplace : propertiesToReplace) {
        if (incoming[propToReplace] && incoming[propToReplace] != target[propToReplace]) {
            target[propToReplace] = incoming[propToReplace]
        }
    }

    for (String propToMerge : propertiesToMerge) {
        if (target[propToMerge] && incoming[propToMerge]) {
            if (incoming[propToMerge] instanceof Map && target[propToMerge] instanceof Map) {
                mergeMap((Map) target[propToMerge], (Map) incoming[propToMerge])
            }
            else if (incoming[propToMerge] instanceof List && target[propToMerge] instanceof List) {
                mergeList((List) target[propToMerge], (List) incoming[propToMerge])
            }
            else if ((incoming[propToMerge] instanceof List && target[propToMerge] instanceof Map) ||
                    (incoming[propToMerge] instanceof Map && target[propToMerge] instanceof List))
            {
                System.err.println("Warning, $propToMerge held mismatching types (Map/List). $propToMerge was not merged.")
            }
        }
    }
}

void mergeList(List into, List from) {
    Set intoSet = into as Set
    for (Object o : from) {
        if (!intoSet.contains(o)) {
            into.add(o)
        }
    }
}

void mergeMap(Map into, Map from) {
    for (Object key : from.keySet()) {
        if (!into.keySet().contains(key)) {
            into[key] = from[key]
        } else { // Both have the key
            if (into[key] instanceof Map && from[key] instanceof Map ) {
                mergeMap( (Map) into[key], (Map) from[key])
            }
            else if (into[key] instanceof List && from[key] instanceof List ) {
                mergeList( (List) into[key], (List) from[key])
            }
            else if ((into[key] instanceof List && from[key] instanceof Map) ||
                    (into[key] instanceof Map && from[key] instanceof List))
            {
                System.err.println("Warning, $key held mismatching types (Map/List). $key was not merged.")
            }
        }
    }
}

void moveToComponent(Map mainEntity, List<String> propertiesToMoveToFirstComponent) {
    if (!mainEntity["hasComponent"])
        mainEntity["hasComponent"] = [[:]]
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