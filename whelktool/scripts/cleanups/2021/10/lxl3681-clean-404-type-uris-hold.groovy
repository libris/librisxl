import whelk.util.DocumentUtil

selectByCollection('hold') { hold ->
    boolean saveMe = false
    def whelk = hold.whelk
    DocumentUtil.traverse(hold.doc.data, { value, path ->
        if (!value || !(value instanceof Map) || !value."@id") {
            return
        }
        String iri = value."@id"
        if (iri.startsWith("https://id.kb.se/marc") && iri.contains("Type-")) {
            if (!whelk.storage.getSystemIdByIri(iri)) {
                incrementStats(iri, path)
                saveMe = true
                return new DocumentUtil.Remove()
            }
        }
    }
    )

    if (saveMe) {
        hold.scheduleSave()
    }
}