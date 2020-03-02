/*
 * Move following properties from Work to Instance and make object an Instance
 *
 * NOTE: This script temporarily keeps marc:displayNote, marc:controlSubfield
 * partNumber, marc:fieldref and marc:groupid and moves them to the Instance entity.
 * These properties will be be cleaned up in the following steps (separate issue LXL-3019)
 *
 * Also move isPartOf.part to thing.part
 *
 * See LXL-1717, LXL-1135, LXL-2777 and LXL-2787
 *
 */

LINK_FIELDS_TO_MOVE = ['isPartOf', 'otherEdition', 'issuedWith', 'dataSource']
NOTE_TO_MOVE = ['hasNote' : 'marc:LinkingEntryComplexityNote']
LEGACY_PROPERTIES = ['marc:toDisplayNote', 'marc:controlSubfield', 'partNumber', 'marc:fieldref', 'marc:groupid', 'marc:displayText']
PROPERTIES_TO_IGNORE = ['marc:displayText', 'part']

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")
deviantRecords = getReportWriter("deviant-records-to-analyze")

String subQuery = LINK_FIELDS_TO_MOVE.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryNote = NOTE_TO_MOVE.collect {"data#>>'{@graph,2,${it.key}}' LIKE '%${it.value}%'"}.join(" OR ")
String query = "collection = 'bib' AND ( ${subQuery} OR ${subQueryNote} )"

selectBySqlWhere(query) { data ->
    def (record, thing, work) = data.graph

    work.subMap(LINK_FIELDS_TO_MOVE + NOTE_TO_MOVE.keySet()).each { key, val ->
        def newListOfObjects
        boolean shouldRemoveProperties = false

        if (key == 'hasNote') {
            newListOfObjects = val.findAll { it[TYPE] == NOTE_TO_MOVE[key] }
            val.removeIf { it[TYPE] == NOTE_TO_MOVE[key] }
        } else {
            if (key == 'isPartOf') {
                shouldRemoveProperties = true
                def part = getPart(val, record[ID])
                if (!part.isEmpty())
                    thing << ['part': part]
            }
            newListOfObjects = updateProperties(val, shouldRemoveProperties, record[ID])
        }

        if (!newListOfObjects.isEmpty()) {
            if (thing.containsKey(key))
                thing[key].addAll(newListOfObjects)
            else
                thing[key] = newListOfObjects
        }
    }

    work.entrySet().removeIf { LINK_FIELDS_TO_MOVE.contains(it.key) ||
            (NOTE_TO_MOVE.containsKey(it.key) && it.value.isEmpty()) }

    scheduledForChange.println "Record was updated ${record[ID]}"
    data.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}

List getPart(listOfObjects, docId) {
    Set parts = []
    listOfObjects.each {
        if (it.hasInstance && it.hasInstance instanceof List)
            it.hasInstance.each { if (it.part) { parts = addUniquePart(it.part, parts, docId) } }
        else if (it.hasInstance?.part)
            parts = addUniquePart(it.hasInstance.part, parts, docId)
    }
    return parts as List
}

Set addUniquePart(part, existingParts, docId) {
    def updatedParts = new LinkedHashSet<String>(existingParts)
    updatedParts.addAll(part)

    if (!existingParts.isEmpty() && updatedParts.size() > existingParts.size())
        deviantRecords.println "${docId} contains more than one unique part"

    return updatedParts
}

List updateProperties(listOfObjects, shouldIgnoreProperties, docID) {
    def newListOfObjects = []

    listOfObjects.each {
        instanceObject = remodelObjectToInstance(it, shouldIgnoreProperties, docID)
        if (instanceObject)
            newListOfObjects << instanceObject
    }
    return newListOfObjects
}

Map remodelObjectToInstance(object, shouldIgnoreProperties, docID) {
    Map instanceProperties = [:]
    Map workProperties = [:]
    Map newInstanceObject = [:]

    if (object.hasInstance instanceof List && object.hasInstance?.size() > 1) {
        deviantRecords.println "${docID} contains more than one hasInstance entity"
        return
    }

    object.each {
        //Check if object already is linked
        if (it.key == 'hasInstance' && it.value instanceof List && it.value.any { it.containsKey(ID) })
            return newInstanceObject = it.value.find { it[ID] }
        else if (it.key == ID)
            return newInstanceObject << it

        if (it.key == 'hasInstance')
            instanceProperties << it.value
        else if (LEGACY_PROPERTIES.contains(it.key))
            instanceProperties << it
        else
            workProperties << it
    }

    if (shouldIgnoreProperties)
        instanceProperties.keySet().removeIf { PROPERTIES_TO_IGNORE.contains(it) }

    if (instanceProperties && !onlyContainsNoise(instanceProperties)) {
        instanceProperties[TYPE] = "Instance"
        newInstanceObject << instanceProperties
    }

    if (workProperties && !onlyContainsNoise(workProperties)) {
        workProperties[TYPE] = "Work"
        newInstanceObject << ['instanceOf': workProperties]
    }

    return newInstanceObject
}

boolean onlyContainsNoise(map) {
    boolean noise = true
    def noiseProperties = LEGACY_PROPERTIES + TYPE
    map.each { key, val ->
        if (!noiseProperties.contains(key))
            noise = false
    }
    return noise
}