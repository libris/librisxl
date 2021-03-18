// This resaves everything, in order to update all the checksums, after the checksum algorithm changed.
// run with --skip-index !

selectBySqlWhere("deleted = false") { data ->
    data.scheduleSave()
}
