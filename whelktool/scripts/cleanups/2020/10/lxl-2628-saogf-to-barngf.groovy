/**
 * Change saogf Ljudböcker to barngf Ljudböcker for anything that has intendedAudience Juvenile
 *
 * See LXL-2628 for more info.
 */

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = """
        collection = 'bib' AND deleted = false AND
        data#>>'{@graph,1,instanceOf,genreForm}' LIKE '%https://id.kb.se/term/saogf/Ljudb%C3%B6cker%' AND
        data#>>'{@graph,1,instanceOf,intendedAudience}' LIKE '%https://id.kb.se/marc/Juvenile%'
"""

selectBySqlWhere(where) { data ->
    boolean changed = false
    def instance = data.graph[1]

    // Paranoia
    if (!instance.instanceOf?.intendedAudience?.any { it["@id"] == "https://id.kb.se/marc/Juvenile" })
        return

    instance.instanceOf?.genreForm?.each { genreForm ->
        if (genreForm["@id"] == "https://id.kb.se/term/saogf/Ljudb%C3%B6cker") {
            genreForm["@id"] = "https://id.kb.se/term/barngf/Ljudb%C3%B6cker"
            changed = true
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
