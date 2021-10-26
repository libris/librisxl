import whelk.util.Statistics
import whelk.util.DocumentUtil

class Script {
    static Statistics s = new Statistics(10)
}

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
                Script.s.increment(iri, path, bib.doc.getShortId())
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