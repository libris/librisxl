/*
 * This remodels bib 041
 *
 * instanceOf.translationOf (041 $k) -> instanceOf.marc:intermediateLanguage
 *
 * instanceOf.originalVersion.language (041 $h)--> instanceOf.translationOf.language
 *
 * See LXL-725 for more info.
 */

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")

selectBySqlWhere("""
        collection = 'bib' AND ( 
            data#>>'{@graph,2,translationOf}' IS NOT NULL OR
            data#>>'{@graph,2,originalVersion}' IS NOT NULL OR
            data#>>'{@graph,1,instanceOf,translationOf}' IS NOT NULL OR
            data#>>'{@graph,1,instanceOf,originalVersion}' IS NOT NULL
        )
    """) { data ->
    boolean changed = false
    def (record, thing, potentialWork) = data.graph
    def work = getWork(thing, potentialWork)

    if (work.containsKey('translationOf')) {
        work['marc:intermediateLanguage'] = work['translationOf']
        work.remove('translationOf')
        changed = true
    }

    if (work.containsKey('originalVersion')) {
        work['translationOf'] = work['originalVersion']
        work.remove('originalVersion')
        changed = true
    }

    if (changed) {
        scheduledForUpdate.println("${record[ID]}")
        data.scheduleSave(onError: { e ->
            failedBibIDs.println("Failed to update ${record[ID]} due to: $e")
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