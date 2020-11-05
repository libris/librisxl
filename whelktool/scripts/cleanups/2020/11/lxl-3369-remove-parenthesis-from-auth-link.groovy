import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter unmapped = getReportWriter("failed-to-map")

ids = ['sb4lrlp45hc5zkf', 'q2thzmc6n26fvjgq', 'j2vbjxtv5s5qpbj', '9tm10vpm3x4mqhr', 'dnw2lrvlbfrll1k5']

selectByIds(ids) { data ->

    boolean modified = DocumentUtil.traverse(data.graph[1], { value, path ->
        if (value instanceof String && value ==~ "https://id.kb.se/term/.*\\(.*\\).*") {

            String correctedUri = value.replace(['(': '%28', ')': '%29'])

            boolean existsCorrected = false

            selectByIds([correctedUri]) {
                existsCorrected = true
            }

            if (existsCorrected) {
                return new DocumentUtil.Replace(correctedUri)
            } else {
                unmapped.println("${data.doc.getURI()}" + "\t" + value)
            }
        }
    })

    if (modified) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
