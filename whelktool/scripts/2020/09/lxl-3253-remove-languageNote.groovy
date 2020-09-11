/*
 * Remove all occurrences of marc:languageNote
 *
 * See LXL-3253 for more information.
 */

failedIDs = getReportWriter("failed-to-delete-bibIDs")
scheduledForChange = getReportWriter("scheduledForChange")

selectByCollection('bib') { bib ->
    Map work = getWork(bib)
    boolean changed = false

    if (!work) {
        return
    }

    if (work.remove('marc:languageNote')) {
        changed = true
    }

    if (work?.hasPart) {
        work.hasPart.each {
            if (it.remove('marc:languageNote')) {
                changed = true
            }
        }
    }

    if (work?.translationOf) {
        if (work.translationOf instanceof Map) {
            work.translationOf = [work.translationOf]
        }
        work.translationOf.each {
            if (it.remove('marc:languageNote')) {
                changed = true
            }
        }
    }

    if (changed) {
        scheduledForChange.println "Record was updated ${bib.graph[0][ID]}"
        bib.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${bib.graph[0][ID]} due to: $e")
        })
    }
}

Map getWork(bib) {
    def (_record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    } else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}