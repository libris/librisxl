/*
 * Move properties from Work to Instance and make object an Instance
 *
 * See LXL-3161
 *
 */

LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo',
                    'continues', 'continuesInPart', 'precededBy', 'precededInPartBy',
                    'mergerOf', 'absorbed', 'absorbedInPart', 'separatedFrom', 'continuedBy',
                    'continuedInPartBy', 'succeededBy', 'succeededInPartBy', 'absorbedBy',
                    'absorbedInPartBy', 'splitInto', 'mergedToForm', 'relatedTo' ]
HAS_PART = 'hasPart'
HAS_INSTANCE = 'hasInstance'
DISPLAY_TEXT = 'marc:displayText'

PrintWriter failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")
deviantRecords = getReportWriter("deviant-records-to-analyze")

String subQueryWork = LINK_FIELDS_WORK.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryHasPart = "data#>>'{@graph,2,${HAS_PART}}' LIKE '%${HAS_INSTANCE}%'"
String query = "collection = 'bib' AND ( ${subQueryWork} OR ${subQueryHasPart} )"

selectBySqlWhere(query) { data ->
    boolean changed = false
    def (record, thing, potentialWork) = data.graph
    def work = getWork(thing, potentialWork)

    if (work == null) {
        return failedIDs.println("Failed to process ${record[ID]} due to missing work entity")
    }

    work.subMap(LINK_FIELDS_WORK + HAS_PART).each { key, val ->
        List newListOfObjects = moveToInstance(key, val, record[ID])

        if (!newListOfObjects.isEmpty()) {
            if (thing.containsKey(key))
                thing[key].addAll(newListOfObjects)
            else
                thing[key] = newListOfObjects
            changed = true
        }
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

List moveToInstance(property, listOfWorkObjects, docID) {
    List newListOfObjects = []
    Iterator iter = listOfWorkObjects.iterator()

    while (iter.hasNext()) {
        Object workEntity = iter.next()
        instanceObject = remodelObjectToInstance(property, workEntity, docID)
        if (instanceObject) {
            newListOfObjects << instanceObject
            iter.remove()
        }
    }

    return newListOfObjects
}

Map remodelObjectToInstance(property, object, docID) {
    Map instanceProperties = [(TYPE):'Instance']
    Map workProperties = [(TYPE):'Work']
    Map newInstanceObject = [:]
    boolean isLinked = false

    if (object.hasInstance && object.hasInstance instanceof List && object.hasInstance?.size() > 1) {
        deviantRecords.println "${docID} contains more than one hasInstance entity"
        return
    }

    if (property == HAS_PART &&
            !(object.containsKey(HAS_INSTANCE) || object.containsKey(DISPLAY_TEXT))) {
        return
    }

    object.each {
        //Check if object already is linked
        if (it.key == 'hasInstance' && it.value instanceof List && it.value.any { it.containsKey(ID) }) {
            isLinked = true
            newInstanceObject = it.value.find { it[ID] }
        } else if (it.key == ID){
            isLinked = true
            newInstanceObject << it
        }

        if (!isLinked) {
            if (it.key == 'hasInstance') {
                instanceProperties << it.value
            } else if (it.key == DISPLAY_TEXT) {
                instanceProperties << it
            } else {
                workProperties << it
            }
        }
    }

    //Move qualifier from Work.hasTitle to Instance.hasTitle
    if (workProperties.containsKey('hasTitle')) {

        if (workProperties['hasTitle'].size() > 1) {
            deviantRecords.println "${docID} contains more than one work hasTitle entity"
        } else {
            def qualifier = getAndRemoveQualifierFromWork(workProperties['hasTitle'])
            if (qualifier) {
                if (!instanceProperties.containsKey('hasTitle')) {
                    instanceProperties['hasTitle'] = [(TYPE): 'Title']
                }
                instanceProperties['hasTitle'][0].put('qualifier', qualifier)
                if ((workProperties['hasTitle'][0].keySet() - TYPE).size() == 0) {
                    workProperties.remove('hasTitle')
                }
            }
        }
    }

    if ((workProperties.keySet() - TYPE).size() > 0) {
        instanceProperties << ['instanceOf': workProperties]
    }

    if ((instanceProperties.keySet() - TYPE).size() > 0) {
        newInstanceObject << instanceProperties
    }

    return newInstanceObject
}

String getAndRemoveQualifierFromWork(titleEntity) {
    def qualifier
    titleEntity.each {
        if (it.containsKey('qualifier')) {
            qualifier = it['qualifier']
            it.remove('qualifier')
        }
    }
    return qualifier
}

Map getWork(thing, work) {
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    } else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}