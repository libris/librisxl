import whelk.util.DocumentUtil

selectByCollection('bib') { bib ->
    def whelk = bib.whelk
    DocumentUtil.traverse(bib.doc.data, { value, path ->
        if (!value || !(value instanceof Map) || !value."@id") {
            return
        }
        String iri = value."@id"
        if (isMarcTermWithType(iri) && !ignore(iri)) {
            if (!whelk.storage.getSystemIdByIri(iri)) {
                incrementStats(iri, path)
                bib.scheduleSave()
                return new DocumentUtil.Remove()
            }
        }
    }
    )
}

private boolean ignore(String iri) {
    ["https://id.kb.se/marc/MapsProjectionType-a"].contains(iri)
}

private boolean isMarcTermWithType(String iri) {
    iri.startsWith("https://id.kb.se/marc") && iri.contains("Type-")
}