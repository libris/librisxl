import whelk.util.DocumentUtil

selectByCollection('bib') { bib ->
    def whelk = bib.whelk
    DocumentUtil.findKey(bib.doc.data, '@id') { iri, path ->
        if (isMarcTermWithType(iri) && !ignore(iri)) {
            if (!whelk.storage.getSystemIdByIri(iri)) {
                incrementStats(iri, path)
                bib.scheduleSave()
                return new DocumentUtil.Remove()
            }
        }
    }
}

private boolean ignore(String iri) {
    ["https://id.kb.se/marc/SerialsRegularityType-u"].contains(iri)
}

private boolean isMarcTermWithType(String iri) {
    iri.startsWith("https://id.kb.se/marc") && iri.contains("Type-")
}