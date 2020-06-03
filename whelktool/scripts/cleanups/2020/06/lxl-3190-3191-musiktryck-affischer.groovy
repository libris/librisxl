/**
 * Fix instance and work types
 *
 * IF
 * work.@type Text
 * instance.@type Instance
 * marc:mediaTerm Musiktryck
 * -->
 * work.@type NotatedMusic
 * instance.@type Print
 *
 * (~2300 records)
 *
 *
 * IF
 * work.@type Text
 * instance.@type Instance
 * marc:mediaTerm %Affisch%
 * -->
 * instance.@type Print
 *
 * (~3000 records)
 *
 *
 * See LXL-3190 and LXL-3191 for more information
 */

PrintWriter updatedReport = getReportWriter("updated.txt")
PrintWriter failedReport = getReportWriter("failed.txt")

selectByCollection('bib') { bib ->
    def instance = bib.graph[1]
    def work = getWork(bib)
    def mediaTerm = instance['marc:mediaTerm']

    if (!work
            || !mediaTerm
            || work['@type'] != 'Text'
            || instance['@type'] != 'Instance'
    ) {
        return
    }

    boolean updated = false
    if (mediaTerm.contains('Affisch')) {
        instance['@type'] == 'Print'
        updatedReport.println("${bib.doc.getURI()} Affisch")
        updated = true
    }
    else if (mediaTerm == 'Musiktryck') {
        work['@type'] == 'NotatedMusic'
        instance['@type'] == 'Print'
        updatedReport.println("${bib.doc.getURI()} Musiktryck")
        updated = true
    }

    if (updated) {
        bib.scheduleSave(onError: { e ->
            failedReport.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
    }
}


Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}