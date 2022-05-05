import whelk.util.DocumentUtil

selectByCollection('bib') { bib ->
    DocumentUtil.findKey(bib.doc.data, "marc:existenceInNlmCollection") { value, path ->
        bib.scheduleSave()
        return new DocumentUtil.Remove()
    }
}