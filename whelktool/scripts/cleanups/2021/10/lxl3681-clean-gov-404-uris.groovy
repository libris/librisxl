import whelk.util.DocumentUtil

selectByCollection('bib') { bib ->
    boolean saveMe = false
    def whelk = bib.whelk
    DocumentUtil.traverse(bib.doc.data, { value, path ->
        if (!value || !(value instanceof Map) || !value."@id") {
            return
        }
        String iri  = value."@id"
        if (iri.contains("Type-")) {
            if (!whelk.storage.getSystemIdByIri(iri)) {
                incrementStats(iri, path)
            }
            saveMe = true

            return new DocumentUtil.Remove()
        }
    }
            //Remove container if empty
    )

    if (saveMe) {
        bib.scheduleSave()
    }
}