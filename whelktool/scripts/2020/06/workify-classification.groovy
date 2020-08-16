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
 * Remain on work but remove itemPortion / marc:itemNumber
 * classification ClassificationUdc (bib 080) (OBS. $b mappad till "marc:itemNumber")
 * classification ClassificationDdc (bib 082)
 * additionalClassificationDdc AdditionaClassificationDdc (bib 083)
 * classification Classification (bib 084)
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
CLASSIFICATION = [ 'classification', 'additionalClassificationDdc', 'marc:hasGeographicClassification' ]

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

    if (work.containsKey('marc:hasGeographicClassification')) {
        thing.put('marc:hasGeographicClassification', work['marc:hasGeographicClassification'])
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

            //itemPortion - for all?
            changed |= removeItemPortion(classificationEntity)

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

boolean removeItemPortion(entity) {
    boolean wasRemoved = false
    if (entity.containsKey('itemPortion')) {
        entity.remove('itemPortion')
        wasRemoved |= true
    }
    if (entity.containsKey('marc:itemNumber')) {
        entity.remove('marc:itemNumber')
        wasRemoved |= true
    }
    return wasRemoved
}

List copyMediaExtensionsDetails(entity, docId) {
    List copyToInstance = []
    if (entity.inScheme?.code == 'kssb' && entity['code'] && entity['code'].contains('/')) {
        //log if it does not follow practice --> will be fixed manually
        def extensions = entity['code'].split("/")
        if (extensions.size() == 2 &&
                (extensions[1].size() == 1 || extensions[1].size() == 2)) {
            copyToInstance << entity.clone()
            entity['code'] = extensions[0]
        } else {
            deviatedMediaExtensions.println("${docId}")
        }
    }
    return copyToInstance
}
