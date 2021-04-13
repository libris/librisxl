/**
 * Trim leading and trailing whitespace + any BOM from doc strings
 */
selectByCollection('bib') { data ->
    if (data.doc.trimStrings()) {
        data.scheduleSave()
    }
}
