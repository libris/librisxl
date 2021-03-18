def storage = getWhelk().storage

selectBySqlWhere('deleted = false') { doc ->
    storage.recalculateDependencies(doc.doc)
}

def getWhelk() {
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}