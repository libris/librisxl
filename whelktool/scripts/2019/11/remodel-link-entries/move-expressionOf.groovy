/*
 * This removes legacy marc properties from link entries
 * and moves expressionOf components to the Work entity.
 *
 * List of ids are generated with:
 * String subQueryWork = LINK_FIELDS_WORK.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
 * String query = "collection = 'bib' AND ( ${subQueryWork} )"
 *
 * See LXL-2833 for more info.
 */

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")
expressionOfpropertiesNotMoved = getReportWriter("properties-not-moved")
File bibIds = new File(scriptDir, 'linkfields-with-expressionOf')

LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo',
                    'hasPart', 'precededBy', 'continues', 'continuesInPart',
                    'precededInPartBy', 'mergerOf', 'absorbed', 'absorbedInPart',
                    'separatedFrom', 'succeededBy', 'continuedBy', 'continuedInPartBy',
                    'succeededInPartBy', 'absorbedBy', 'absorbedInPartBy', 'splitInto',
                    'mergedToForm', 'dataSource', 'relatedTo', 'isPartOf',
                    'otherEdition', 'issuedWith' ]


selectByIds(bibIds.readLines()) { data ->
    def (record, thing, work) = data.graph

    work.subMap(LINK_FIELDS_WORK).each {
        updateProperties(data, it.getKey(), it.getValue())
    }
}

void updateProperties(data, propertyName, toUpdate) {
    boolean expressionOfWasUpdated
    objectsToUpdate = (toUpdate instanceof List) ? toUpdate : [toUpdate]

    objectsToUpdate.each {
        if (propertyName == 'isPartOf' && it[TYPE] != 'Aggregate')
            return
        expressionOfWasUpdated = moveExpressionOf(it, data)
    }

    if (expressionOfWasUpdated) {
        scheduledForChange.println "Record was updated ${data.graph[0][ID]}"
        data.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${data.graph[0][ID]} due to: $e")
        })
    }
}

boolean moveExpressionOf(object, data) {
    boolean wasChanged = false
    object.remove('expressionOf')?.findAll { key, value ->
        if (!object.containsKey(key)) {
            object[key] = value
            wasChanged = true
        } else {
            if (!object[key] == TYPE)
                expressionOfpropertiesNotMoved.println "${key} already exists for ${data.graph[0][ID]}"
        }
    }
    return wasChanged
}