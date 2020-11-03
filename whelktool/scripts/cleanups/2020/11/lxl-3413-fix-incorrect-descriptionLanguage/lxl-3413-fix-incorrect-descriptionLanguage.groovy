PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

List ids = new File(scriptDir, 'data-1601623118108.txt').readLines().drop(1).collect{line -> line.split('\t')[1]}

selectByIds(ids) { auth ->

    def descriptionLang = auth.graph[0].descriptionLanguage

    if (!(descriptionLang['@id'] == 'https://id.kb.se/language/swe')) {
        descriptionLang = ['@id':'https://id.kb.se/language/swe']

        scheduledForUpdating.println("${auth.doc.getURI()}")
        auth.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${auth.doc.shortId} due to: $e")
        })
    }
}