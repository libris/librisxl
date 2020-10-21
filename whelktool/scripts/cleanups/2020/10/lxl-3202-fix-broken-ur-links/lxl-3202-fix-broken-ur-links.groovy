/**
 * Fix broken UR links (http://www.ur.se/id/<id> => https://www.urplay.se/program/<id>)
 *
 * See LXL-3202 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

File bibIDsFile = new File(scriptDir, 'lxl-3202-ids.txt')

selectByIds(bibIDsFile.readLines()) { bib ->
    boolean changed = false
    def instance = bib.graph[1]

    if (!instance.relatedTo)
        return

    instance.relatedTo.each { relatedTo ->
        if (relatedTo.uri instanceof List) {
            for (int i = 0; i < relatedTo.uri.size(); ++i) {
                if (relatedTo.uri[i] instanceof String && relatedTo.uri[i].startsWith("http://www.ur.se/id/")) {
                    relatedTo.uri[i] = relatedTo.uri[i].replaceFirst("http://www.ur.se/id/", "https://www.urplay.se/program/")
                    changed = true
                }
            }
        }
    }

    if (changed) {
        scheduledForUpdating.println("${bib.doc.getURI()}")
        bib.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
    }
}
