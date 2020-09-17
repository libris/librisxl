/*
 * This remodels bib 041
 *
 * instanceOf.hasPart.translationOf (041 $k) -> instanceOf.hasPart.marc:intermediateLanguage
 * instanceOf.hasPart.originalVersion.language (041 $h)--> instanceOf.hasPart.translationOf.language
 *
 * See LXL-3292 for more info.
 */

PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")

selectBySqlWhere("""
    collection = 'bib' AND (
        data#>>'{@graph,1,instanceOf,hasPart}' IS NOT NULL
    )
""") { data ->

    boolean changed = false
    def (record, thing, potentialWork) = data.graph
    def work = getWork(thing, potentialWork)

    work.hasPart.each {
        if (it.containsKey('translationOf')) {
            it['marc:intermediateLanguage'] = it['translationOf']
            it.remove('translationOf')
            changed = true
        }

        if (it.containsKey('originalVersion')) {
            it['translationOf'] = it['originalVersion']
            it.remove('originalVersion')
            changed = true
        }
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