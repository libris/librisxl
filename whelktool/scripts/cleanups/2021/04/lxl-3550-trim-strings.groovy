/**
 * Trim leading and trailing whitespace from doc strings
 */
selectByCollection('bib') { data ->
    if (data.doc.trimStrings()) {
        data.scheduleSave()
    }
}
selectByCollection('auth') { data ->
    if (data.doc.trimStrings()) {
        data.scheduleSave()
    }
}