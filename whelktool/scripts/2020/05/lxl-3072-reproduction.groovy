/*
 * This remodels hasReproduction to production.Reproduction
 *
 * hasReproduction.provisionActivity.ProvisionActivity -> production.Reproduction
 * hasReproduction.description -> production.Reproduction.typeNote
 * hasReproduction.hasNote -> production.Reproduction.hasNote
 *
 * hasReproduction.extent -> mainEntity.extent ($e)
 * hasReproduction.seriesStatement -> mainEntity.seriesStatement ($f) ?
 * hasReproduction.appliesTo -> mainEntity.appliesTo ($3)
 * hasReproduction.applicableInstitution -> mainEntity.applicableInstitution ($5)
 * hasReproduction.marc:fixedLengthDataElementsOfReproduction -> mainEntity.marc:fixedLengthDataElementsOfReproduction ($7)
 *
 * hasReproduction.marc:datesAndOrSequentialDesignationOfIssuesReproduced -> mainEntity.hasNote ($m) ???
 *
 * See LXL-3072 for more info.
 */

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
deviantRecords = getReportWriter("deviant-records-to-analyze")

selectBySqlWhere("""
        collection = 'bib' AND data#>>'{@graph,1,hasReproduction}' IS NOT NULL
    """) { data ->
    def (record, mainEntity) = data.graph
    boolean changed = false
    def currentObject = mainEntity['hasReproduction']
    def newObject = []

    newObject.addAll(currentObject.collect { toProduction(mainEntity, it, record[ID]) })

    if (!((newObject - null).size() == 0)) {
        mainEntity['production'] = newObject
        mainEntity.remove('hasReproduction')
        changed = true
    }

    if (changed) {
        scheduledForUpdate.println("${record[ID]}")
        data.scheduleSave(onError: { e ->
            failedBibIDs.println("Failed to update ${record[ID]} due to: $e")
        })
    }
}

Map toProduction(mainEntity, current, docId) {
    Map reproduction = [(TYPE): 'Reproduction']

    current.each { key, value ->
        switch(key) {
            case 'hasNote':
            case 'marc:fixedLengthDataElementsOfReproduction':
                reproduction << [(key):value]
                break

            case 'description':
                reproduction << ['typeNote':value]
                break

            case 'provisionActivity':
                if (value.size() > 1) {
                    deviantRecords.println "${docId} contains more than one provisionActivity"
                    break
                }
                reproduction << value[0].findAll { it.key != TYPE }
                break

            case 'marc:datesAndOrSequentialDesignationOfIssuesReproduced': //5 occurrences
                if (!mainEntity.containsKey('hasNote')) {
                    mainEntity['hasNote'] = []
                }
                mainEntity['hasNote'].addAll( value.collect { [(TYPE): 'Note', 'label': it ] } )
                break

            case 'extent':
            case 'seriesStatement':
                if (mainEntity.containsKey(key) && mainEntity[key] instanceof Map) {
                    deviantRecords.println "${docId} contains ${key} that´s not a list"
                    break
                }
                if (!mainEntity.containsKey(key)) {
                    mainEntity[key] = []
                }
                mainEntity[key].addAll(value)
                break

            case 'appliesTo': // 1 occurrences
            case 'applicableInstitution': //13 occurrences
                if (mainEntity.containsKey(key)) {
                    deviantRecords.println "${docId} already contains ${key}"
                } else {
                    mainEntity[key] = value
                }
                break

            case 'production': // 2 occurrences
            case 'uri': // 1 occurrences
            case 'provisionActivityStatement': // 1 occurrences
            case '@type':
                // drop
                break

            default:
                deviantRecords.println "${docId} contains unhandled property: $key : $value"
        }
    }
    reproduction = ((reproduction.keySet() - TYPE).size() == 0) ? null : reproduction
    return reproduction
}


