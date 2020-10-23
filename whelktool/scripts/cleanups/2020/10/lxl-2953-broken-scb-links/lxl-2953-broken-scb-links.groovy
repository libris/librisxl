import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

Map scbData = [:]

File scbFile = new File(scriptDir, 'SCB_nya_länkar.txt')

scbFile.withReader {reader ->
    reader.readLine() // Skip column headers
    reader.eachLine {line ->
        columns = line.split()
        scbData[columns[0]] = ['oldUrl':columns[1], 'newUrl':columns[2]]
    }
}

String idsStr = "\'" + scbData.keySet().join("\',\'") + "\'"

// Identify entries by controlNumber (corresponding to LibrisID in 'SCB_nya_länkar.txt')
String where = "collection = 'bib' AND data#>>'{@graph,0,controlNumber}' IN ($idsStr)"

selectBySqlWhere(where) { bib ->
    def id = bib.graph[0]['controlNumber']

    // Find old url and replace it with new
    boolean modified = DocumentUtil.traverse(bib.graph[1], {value, path ->
        if (value == scbData[id]['oldUrl']) {
            return new DocumentUtil.Replace(scbData[id]['newUrl'])
        }
    })

    if (modified) {
        scheduledForUpdating.println("${bib.doc.getURI()}")
        bib.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${bib.doc.shortId} due to: $e")
        })
    }
}