/*
 * This removes legacy marc properties from link entries
 *
 * See LXL-3019 for more info.
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")

PROPERTIES_TO_REMOVE = ['marc:controlSubfield', 'marc:groupid', 'partNumber', 'marc:toDisplayNote', 'marc:fieldref']
LINK_FIELDS_INSTANCE = ['hasSeries', 'hasSubseries', 'isPartOf', 'otherEdition',
                        'otherPhysicalFormat', 'issuedWith', 'dataSource']
LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo',
                    'precededBy', 'continues', 'continuesInPart', 'precededInPartBy',
                    'mergerOf', 'absorbed', 'absorbedInPart', 'separatedFrom', 'succeededBy',
                    'continuedBy', 'continuedInPartBy', 'succeededInPartBy', 'absorbedBy',
                    'absorbedInPartBy', 'splitInto', 'mergedToForm', 'relatedTo' ]
HAS_PART = 'hasPart'
HAS_INSTANCE = 'hasInstance'

String subQueryInstance = LINK_FIELDS_INSTANCE.collect {"data#>>'{@graph,1,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryWork = LINK_FIELDS_WORK.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryHasPart = "data#>>'{@graph,2,${HAS_PART}}' LIKE '%${HAS_INSTANCE}%'"
String query = "collection = 'bib' AND ( ${subQueryInstance} OR ${subQueryWork} OR ${subQueryHasPart} )"

selectBySqlWhere(query) { data ->
    boolean changed = false
    def record = data.graph[0]
    def thing = data.graph[1]
    def work = thing.instanceOf

    thing.subMap(LINK_FIELDS_INSTANCE).each { key, val ->
        if (findAndRemoveLegacyProperties(val, key) && !changed)
            changed = true
    }
    //If empty after removing legacy properties, remove property
    thing.entrySet().removeIf { LINK_FIELDS_INSTANCE.contains(it.key) && it.value.size() == 0 }

    work.subMap(LINK_FIELDS_WORK + HAS_PART).each { key, val ->
        if (findAndRemoveLegacyProperties(val, key) && !changed)
            changed = true
    }
    //If empty after removing legacy properties, remove property
    work.entrySet().removeIf { (LINK_FIELDS_WORK.contains(it.key) || it.key == HAS_PART) &&
            it.value.size() == 0 }

    if (changed) {
        scheduledForChange.println "Record was updated ${record[ID]}"
        data.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${record[ID]} due to: $e")
        })
    }
}

boolean findAndRemoveLegacyProperties(linkFieldValue, propertyName) {
    boolean somethingWasRemoved = false

    if (linkFieldValue instanceof Map) {
        somethingWasRemoved = removeLegacyProperies(linkFieldValue, propertyName)
        if (somethingWasRemoved && (linkFieldValue.keySet() - TYPE).size() == 0 )
            linkFieldValue.clear()
    } else if (linkFieldValue instanceof List) {
        def listIter = linkFieldValue.listIterator()
        for (iter in listIter)  {
            boolean wasRemovedFromEntity = removeLegacyProperies(iter, propertyName)
            //Only @type remains - remove the entity completely
            if (wasRemovedFromEntity && (iter.keySet() - TYPE).size() == 0 )
                listIter.remove()

            if (wasRemovedFromEntity && !somethingWasRemoved)
                somethingWasRemoved = true
        }
    }
    return somethingWasRemoved
}

boolean removeLegacyProperies(object, propertyName) {
    // If hasPart not contains hasInstance we asume the hasPart belongs to marc 700/710/711
    // and should not be updated
    if (propertyName == HAS_PART && !object.containsKey(HAS_INSTANCE))
        return false
    return object.entrySet().removeIf { PROPERTIES_TO_REMOVE.contains(it.key) }
}
