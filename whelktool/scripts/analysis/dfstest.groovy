import whelk.util.DocumentUtil

selectByCollection('bib') { bib ->
    for (int i = 0 ; i < 300 ; i++) {
        DocumentUtil.traverse(bib.doc.data, (v, p) -> { })
    }
}