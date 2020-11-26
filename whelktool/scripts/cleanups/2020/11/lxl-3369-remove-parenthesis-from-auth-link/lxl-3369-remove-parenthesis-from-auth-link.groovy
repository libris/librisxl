import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter unmapped = getReportWriter("failed-to-map")

List ids = new File(scriptDir, 'have-link-to-parenthesis-uri.txt').readLines()

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
