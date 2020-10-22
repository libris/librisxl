import whelk.util.DocumentUtil

String BIB_ID_FILE = 'Springer.txt'

List IDs = new File(scriptDir, BIB_ID_FILE).readLines()

// Change ids of type '123456789' to 'http://libris.kb.se/bib/123456789'
processIds(IDs)

selectByIds(IDs) { bib ->
    // Correct urls in bib
    correctUrls(bib.graph[1])
    saveChanges(bib)

    // Find hold associated with bib id
    def bibId = bib.graph[1]['@id'];

    String where = """id in (select id from lddb 
            where data#>>'{@graph,1,itemOf,@id}' = '$bibId') AND 
            data#>>'{@graph,1}' LIKE '%www.springerlink.com%' AND
            collection = 'hold'"""

    selectBySqlWhere(where) { hold ->
        // Correct urls in hold
        correctUrls(hold.graph[1])
        saveChanges(hold)
    }
}

def processIds(List<String> idList) {
    idList.eachWithIndex{ it, i ->
        if (it.isNumber()) {
            idList[i] = 'http://libris.kb.se/bib/' + it
        }
    }
}

def correctUrls(LinkedHashMap<String,String> data) {
    // Springerlink urls appear in many different paths, therefore traverse the whole graph
    DocumentUtil.traverse(data, {value, path ->
        if (value instanceof String) {
            if (value.contains("www.springerlink.com")) {
                newUrl = value.replace("www.springerlink.com", "link.springer.com")
                return new DocumentUtil.Replace(newUrl)
            }
        }
    })

}

def saveChanges(data) {
    PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
    PrintWriter failedUpdating = getReportWriter("failed-updates")

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}