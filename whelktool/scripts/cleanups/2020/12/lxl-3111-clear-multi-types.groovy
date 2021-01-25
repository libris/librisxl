// This resaves records where there incorrectly are lists of types under hasPart.
// Other changes to record normalization will deal with actually fixing the records.

selectBySqlWhere("data#>>'{@graph,1,instanceOf,hasPart,0,@type,1}' is not null and deleted = false") { data ->
    data.scheduleSave()
}
