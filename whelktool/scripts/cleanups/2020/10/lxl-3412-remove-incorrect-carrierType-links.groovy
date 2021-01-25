import whelk.util.DocumentUtil

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

def incorrectUris = ['https://id.kb.se/marc/KitMaterialType-', 'https://id.kb.se/marc/BooksItemType-',
                      'https://id.kb.se/marc/ItemType-']

selectByCollection('bib') { data ->

    boolean modified1 = false
    boolean modified2 = false

    def carrierType = data.graph[1].carrierType
    def hasPart = data.graph[1].hasPart

    if (carrierType) {
        // Remove incorrect uris in path {@graph,1,carrierType}
        modified1 = removeUris(carrierType, incorrectUris)

        // Remove carrierType completely if it links to nothing
        if (carrierType.isEmpty()) {
            data.graph[1].remove('carrierType')
        }
    }

    if (hasPart && hasPart.carrierType) {
        // Remove incorrect uris in {@graph,1,hasPart,carrierType}
        modified2 = removeUris(hasPart.carrierType, incorrectUris)

        // Remove carrierType completely if it links to nothing
        if (hasPart.carrierType.isEmpty()) {
            hasPart.remove('carrierType')
        }
        // Remove hasPart completely if it links to nothing
        if (hasPart.isEmpty()) {
            data.graph[1].remove('hasPart')
        }
    }


    if (modified1 || modified2) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

def boolean removeUris(Object data, List<String> uris) {
    return DocumentUtil.traverse(data, {value, path ->
        if (value instanceof String && startsWithAny(uris, value)) {
            return new DocumentUtil.Remove()
        }
    })
}

def boolean startsWithAny(List<String> uris, String compareString) {
    for (uri in uris) {
        if (compareString.startsWith(uri)) {
            return true
        }
    }
    return false
}
