/*
 * This removes legacy marc properties from link entries
 * and moves expressionOf components to the Work entity.
 *
 * See LXL-2833 for more info.
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")

LINK_FIELDS_INSTANCE = ['hasSeries', 'hasSubseries']
LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo',
                    'hasPart', 'precededBy', 'continues', 'continuesInPart',
                    'precededInPartBy', 'mergerOf', 'absorbed', 'absorbedInPart',
                    'separatedFrom', 'succeededBy', 'continuedBy', 'continuedInPartBy',
                    'succeededInPartBy', 'absorbedBy', 'absorbedInPartBy', 'splitInto',
                    'mergedToForm', 'dataSource', 'relatedTo', 'isPartOf',
                    'otherEdition', 'issuedWith' ]

String subQueryInstance = LINK_FIELDS_INSTANCE.collect {"data#>>'{@graph,1,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryWork = LINK_FIELDS_WORK.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
String query = "collection = 'bib' AND ( ${subQueryInstance} OR ${subQueryWork} )"

selectBySqlWhere(query) { data ->
    def (record, thing, work) = data.graph

    thing.subMap(LINK_FIELDS_INSTANCE).each {
        updateProperties(data, it.getKey(), it.getValue(), false)
    }

    work.subMap(LINK_FIELDS_WORK).each {
        updateProperties(data, it.getKey(), it.getValue(), true)
    }
}

void updateProperties(data, propertyName, toUpdate, isWork) {
    boolean someThingWasRemoved
    boolean expressionOfWasUpdated
    objectsToUpdate = (toUpdate instanceof List) ? toUpdate : [toUpdate]

    objectsToUpdate.each {
        if (propertyName == 'isPartOf' && it[TYPE] != 'Aggregate')
            return
        expressionOfWasUpdated = isWork ? moveExpressionOf(it) : false
    }

    if (expressionOfWasUpdated) {
        scheduledForChange.println "Record was updated ${data.graph[0][ID]}"
        data.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${data.graph[0][ID]} due to: $e")
        })
    }
}

boolean moveExpressionOf(object) {
    valuesToMove = object['expressionOf'] ? object.expressionOf.findAll { it.key != TYPE } : null
    if (valuesToMove) {
        object << valuesToMove
        object.remove('expressionOf')
        return true
    }
    return false
}