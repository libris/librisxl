/*
 * This moves certain classifications from work to instance, remove
 * itemPortion and marc:itemNumber and handle SAB classification with
 * media extensions
 *
 * Move:
 * classification ClassificationLcc (bib 050)
 * marc:hasGeographicClassification marc:GeographicClassification (bib 052)
 * classification ClassificationNlm (bib 060)
 * classification marc:NationalAgriculturalLibraryCallNumber (bib 070)
 *
 * Remove:
 * marc:existenceInLCCollection (ind1) from classification ClassificationLcc (bib 050)
 * all occurrences of itemPortion / marc:itemNumber
 *
 * SAB classification:
 * classification Classification (bib 084) with inScheme set to SAB -> if code ends with
 * /xx -> copy the entire entity to thing and remove everything after '/'
 *
 * See LXL-3254, LXL-3256 and LXL-3258 for more info.
 */

PrintWriter scheduledForChange = getReportWriter("scheduled-for-update")
PrintWriter failedIDs = getReportWriter("failed-to-update")
deviatedMediaExtensions = getReportWriter("deviatedMediaExtensions")

TYPES_TO_INSTANCE = ['ClassificationLcc',
                     'ClassificationNlm',
                     'marc:NationalAgriculturalLibraryCallNumber']
CLASSIFICATION = ['classification', 'additionalClassificationDdc', 'marc:hasGeographicClassification']
PROPERTIES_TO_REMOVE = ['itemPortion', 'marc:itemNumber', 'marc:existenceInLCCollection']

String subQueryWork = CLASSIFICATION.collect {"data#>>'{@graph,2,${it}}' IS NOT NULL"}.join(" OR ")
String subQueryLocalWork = CLASSIFICATION.collect {"data#>>'{@graph,1,instanceOf,${it}}' IS NOT NULL"}.join(" OR ")
String query = "collection = 'bib' AND ( ${subQueryWork} OR ${subQueryLocalWork} )"

selectBySqlWhere(query) { data ->
    boolean changed = false
    def (record, thing, potentialWork) = data.graph
    def work = getWork(thing, potentialWork)
    List classificationsToInstance = []

    if (work == null) {
        return failedIDs.println("Failed to process ${record[ID]} due to missing work entity")
    }

    //Move and remodel marc:hasGeographicClassification
    if (work.containsKey('marc:hasGeographicClassification')) {
        thing.put('geographicCoverage', remodelGeographicClassifcation(work['marc:hasGeographicClassification']))
        work.remove('marc:hasGeographicClassification')
        changed = true
    }

    work.subMap(CLASSIFICATION).each { key, val ->
        if (val instanceof Map) {
            val = [val]
        }

        Iterator iter = val.iterator()
        while (iter.hasNext()) {
            Object classificationEntity = iter.next()

            //Remove certain properties
            changed |= classificationEntity.keySet().removeIf { PROPERTIES_TO_REMOVE.contains(it) }

            //copy kssb --> classification Classification
            classificationsToInstance.addAll(copyMediaExtensionsDetails(classificationEntity, record[ID]))

            //move --> 'ClassificationLcc', 'ClassificationNlm', 'marc:NationalAgriculturalLibraryCallNumber'
            if (TYPES_TO_INSTANCE.contains(classificationEntity[TYPE])) {
                classificationsToInstance << classificationEntity
                iter.remove()
                changed |= true
            }
        }
    }

    if (!classificationsToInstance.isEmpty()) {
        if (!thing.containsKey('classification')) {
            thing.put('classification', [])
        }
        thing['classification'].addAll(classificationsToInstance)
        changed |= true
    }

    //If empty after removing entities from classification, remove entire entry
    work.entrySet().removeIf { it.key == 'classification' && it.value.size() == 0 }

    if (changed) {
        scheduledForChange.println "Record was updated ${record[ID]}"
        data.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${record[ID]} due to: $e")
        })
    }
}

Map getWork(thing, work) {
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    } else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}

List remodelGeographicClassifcation(object) {
    if (object instanceof Map) {
        object = [object]
    }
    List updatedEntities = object.collect {
        it[(TYPE)] = 'GeographicCoverage'
        it['label'] = it['marc:geographicClassificationAreaCode']
        it.remove('marc:geographicClassificationAreaCode')
        return it
    }
    return updatedEntities
}

List copyMediaExtensionsDetails(entity, docId) {
    List copyToInstance = []
    if (entity.inScheme?.code == 'kssb' && entity['code'] && entity['code'].contains('/')) {
        //log if it does not follow practice --> will be fixed manually
        def extensions = entity['code'].split("/")
        if (extensions.size() == 2 && extensions[1] =~ /^[A-Za-z]{1,2}(,u(f|g)?)?$/) {
            copyToInstance << entity.clone()
            entity['code'] = extensions[0]
        } else {
            deviatedMediaExtensions.println("${docId} with code ${entity['code']}")
        }
    }
    return copyToInstance
}
